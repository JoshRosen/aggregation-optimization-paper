package edu.berkeley.cs.amplab.aggregationsketches

import com.bizo.mighty.csv.CSVWriter
import com.carrotsearch.sizeof.RamUsageEstimator
import edu.berkeley.cs.amplab.aggregationsketches.aggregators._

object AggregationSketches {

  def main(args: Array[String]) {
    val numItems = 20000
    val maxKey = 40000
    val dataGenerators = Seq(1.3).map{ alpha =>
      "Zipf (alpha=%s)".format(alpha) -> DataGenerators.zipf(alpha, maxKey = maxKey)
    }.toMap
    // "All Unique" -> DataGenerators.countFrom(1)
    // TODO: log the number of unique keys and some other properties of the dataset

    val output = CSVWriter(System.out)
    // Outputs results as TSV, which can be pasted into Excel for analysis:
    val columnNames = Seq("Data Set", "Num Items", "Output Size", "Time (ms)", "Extra Space Usage (Bytes)", "Eviction Strategy")
    output.write(columnNames)

    for ((generatorName, generator) <- dataGenerators) yield {
      val items = generator.take(numItems)
      val numUniqueKeys = items.toSet.size
      implicit def aggregationFunction(a: Int, b: Int): Int = a + b
      val aggregationPlans: Seq[AggregationPlan[Int, Int]] = Seq(
        {
          val sink = new CountAggregator
          val root = new RandomBufferingPreAggregator[Int, Int](numItems, sink)
          new AggregationPlan("Complete pre-aggregation", root, sink)
        },
        {
          val sink = new CountAggregator
          new AggregationPlan("No pre-Aggregation", sink, sink)
        },
        {
          val sink = new CountAggregator
          val root = new LRUBufferingPreAggregator[Int, Int](math.round(0.01f * numUniqueKeys), sink)
          new AggregationPlan("1% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val lru = new LRUBufferingPreAggregator[Int, Int](math.round(0.01f * numUniqueKeys), sink)
          val root = new BloomFilterRouter[Int, Int](1000, 0.01f, 0.5f)(lru, sink)
          new AggregationPlan("1000-key Bloom Filter + 1% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val root = new CountMinSketchBufferingPreAggregator[Int, Int](0.01, 1E-3)(math.round(0.01f * numUniqueKeys), sink)
          new AggregationPlan("1% CMS Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val root = new FIFOBufferingPreAggregator[Int, Int](math.round(0.01f * numUniqueKeys), sink)
          new AggregationPlan("1% FIFO Buffer", root, sink)
        }
      )

      val stats = aggregationPlans.iterator.map {plan =>
        System.gc()
        val startTime = System.currentTimeMillis()
        items.map((_, 1)).foreach(plan.rootAggregator.send)
        val endTime = System.currentTimeMillis()
        // For now, measure memory usage before the final close() call.
        // We'll probably want to remove this code if we want exact timing measurements in pipelines with
        // large initial buffers (or a set of custom methods for recursively determining the memory usage
        // of a plan, normalized in units of buffer space.
        val aggregatorMemoryUsage = RamUsageEstimator.sizeOf(plan)
        plan.rootAggregator.close()
        val outputSize = plan.sinkAggregator.asInstanceOf[CountAggregator].getCount
        (plan.name, outputSize, endTime - startTime, aggregatorMemoryUsage)
      }

      for ((policyName, numOutputTuples, time, policySize) <- stats) {
        output.write(Seq(generatorName, numItems, numOutputTuples, time, policySize, policyName).map(_.toString))
        output.flush()
      }
    }
  }
}
