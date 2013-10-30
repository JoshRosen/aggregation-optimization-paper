package edu.berkeley.cs.amplab.aggregationsketches

object AggregationSketches {

  def simulateCountByKey[K](stream: TraversableOnce[K], bufferSize: Int, policy: EvictionPolicy[K]): Int = {
    val writer = new CountingOutputWriter
    val aggregator = new Aggregator[K, Int, Int](bufferSize, policy, writer, x => x, _ + _, _ + _)
    aggregator.aggregateStream(stream.map(x => (x, 1)))
    aggregator.close()
    writer.getCount
  }

  def main(args: Array[String]) {
    val numItems = 10000
    val maxKey = 1000
    val bufferPercentages = Seq(0.01, 0.05, .10, .25, .50, 1.0)
    val dataGenerators = Seq(1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7).map{alpha =>
      "Zipf (alpha=%s)".format(alpha) -> DataGenerators.zipf(alpha, maxKey = maxKey)
    }.toMap
    // "All Unique" -> DataGenerators.countFrom(1)
    // Outputs results as TSV, which can be pasted into Excel for analysis:
    println("%-17s\t%-15s\t%-15s\t%-15s\t%s".format("Data Set", "Fraction Buffered", "Output Size", "Time (ms)", "Eviction Strategy"))
    val outputFormatString = "%17s\t%15.02f\t%15d\t%15d\t%s"
    // TODO: log the number of unique keys and some other properties of the dataset
    for ((generatorName, generator) <- dataGenerators;
         bufferPct <- bufferPercentages) yield {
      val bufferSize = math.round(numItems * bufferPct).toInt
      val items = generator.take(numItems)
      val evictionPolicies = Seq(
        new NoPreAggregationEvictionPolicy[Int],
        new OptimalEvictionPolicy[Int](items),
        new RandomEvictionPolicy[Int],
        new RandomEvictionPolicy[Int] with BloomFilterInitialBypass[Int] { override def numEntries = maxKey },
        new CountMinSketchEvictionPolicy[Int](0.01, 1E-3),
        new CountMinSketchEvictionPolicy[Int](0.01, 1E-3) with BloomFilterInitialBypass[Int] { override def numEntries = maxKey }
      )
      val stats = evictionPolicies.iterator.map{ policy =>
        val startTime = System.currentTimeMillis()
        val numOutputTuples = simulateCountByKey[Int](items, bufferSize, policy)
        (policy.toString, numOutputTuples, System.currentTimeMillis() - startTime)
      }
      for ((policyName, numOutputTuples, time) <- stats) {
        println(outputFormatString.format(generatorName, bufferPct, numOutputTuples, time, policyName))
      }
    }
  }
}
