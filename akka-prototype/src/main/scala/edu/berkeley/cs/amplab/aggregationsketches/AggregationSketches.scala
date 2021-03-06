package edu.berkeley.cs.amplab.aggregationsketches

import com.bizo.mighty.csv.CSVWriter
import com.carrotsearch.sizeof.RamUsageEstimator
import edu.berkeley.cs.amplab.aggregationsketches.aggregators._

object AggregationSketches {

  def main(args: Array[String]) {
    val numItems = 1000000
    val maxKey   = 500000
    val dataGenerators = (Seq(1.1, 1.3, 1.5, 1.7).map{ alpha =>
      "Zipf (alpha=%s)".format(alpha) -> DataGenerators.zipf(alpha, maxKey = maxKey)
    } ++ Seq(
      "Uniform" -> DataGenerators.uniform(maxKey),
      "80% Heavy Hitter" -> DataGenerators.heavyHitter(0.8f),
      "50% Heavy Hitter" -> DataGenerators.heavyHitter(0.5f),
      "20% Heavy Hitter" -> DataGenerators.heavyHitter(0.2f)
    )).toMap
    // "All Unique" -> DataGenerators.countFrom(1)
    // TODO: log the number of unique keys and some other properties of the dataset

    val output = CSVWriter(System.out)
    // Outputs results as TSV, which can be pasted into Excel for analysis:
    val columnNames = Seq("Data Set", "Num Items", "Num Unique Keys", "Num Singletons", "Output Size", "Output Percentage", "Time (ms)",
      "RamUsageEstimator (Bytes)", "Back-of-envelope memory usage (Bytes)", "Aggregation Plan")
    output.write(columnNames)

    for ((generatorName, generator) <- dataGenerators) yield {
      println("\n\nGenerating for generator " + generatorName)
      val items = generator.take(numItems)
      val numUniqueKeys = items.toSet.size
      val numSingletonKeys = items.groupBy(identity).mapValues(_.size).count(_._2 == 1)
      implicit def aggregationFunction(a: Int, b: Int): Int = a + b
      val aggregationPlans: Seq[AggregationPlan[Int, Int]] = Seq(
        {
          val sink = new CountAggregator
          val root = new RandomBufferingPreAggregator[Int, Int](numUniqueKeys, sink)
          new AggregationPlan("Complete pre-aggregation", root, sink)
        },
        {
          val sink = new CountAggregator
          new AggregationPlan("No pre-Aggregation", sink, sink)
        },
        {
          val sink = new CountAggregator
          val root = new LRUBufferingPreAggregator[Int, Int](math.round(0.20f * numUniqueKeys), sink)
          new AggregationPlan("20% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val root = new LRUBufferingPreAggregator[Int, Int](math.round(0.40f * numUniqueKeys), sink)
          new AggregationPlan("40% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val lru = new LRUBufferingPreAggregator[Int, Int](math.round(0.20f * numUniqueKeys), sink)
          val root = new BloomFilterRouter[Int, Int](1000, 0.01f, 0.5f)(lru, sink)
          new AggregationPlan("1000-key Bloom Filter + 20% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val lru = new LRUBufferingPreAggregator[Int, Int](math.round(0.20f * numUniqueKeys), sink)
          val root = new BloomFilterRouter[Int, Int](10000, 0.01f, 0.5f)(lru, sink)
          new AggregationPlan("10000-key Bloom Filter + 20% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val lru = new LRUBufferingPreAggregator[Int, Int](math.round(0.20f * numUniqueKeys), sink)
          val root = new BloomFilterRouter[Int, Int](numUniqueKeys, 0.01f, 0.5f)(lru, sink)
          new AggregationPlan("#uniqueKeys Bloom Filter + 20% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val lru = new LRUBufferingPreAggregator[Int, Int](math.round(0.10f * numUniqueKeys), sink)
          val root = new BloomFilterRouter[Int, Int](numUniqueKeys, 0.01f, 0.5f)(lru, sink)
          new AggregationPlan("#uniqueKeys Bloom Filter + 10% LRU Buffer", root, sink)
        },
        {
          val sink = new CountAggregator
          val root = new FIFOBufferingPreAggregator[Int, Int](math.round(0.20f * numUniqueKeys), sink)
          new AggregationPlan("20% FIFO Buffer", root, sink)
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
        (plan, outputSize, endTime - startTime, aggregatorMemoryUsage)
      }

      for ((plan, numOutputTuples, time, policySize) <- stats) {
        output.write(Seq(generatorName, numItems, numUniqueKeys, numSingletonKeys, numOutputTuples, numOutputTuples * 1.0f / numItems, time, policySize, plan.memoryUsageInBytes, plan.name).map(_.toString))
        output.flush()
      }
    }
  }
}
