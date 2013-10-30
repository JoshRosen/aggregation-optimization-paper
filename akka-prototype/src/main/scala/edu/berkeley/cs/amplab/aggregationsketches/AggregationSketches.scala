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
    val maxKey = 10000
    val generator = DataGenerators.zipf(1.1, maxKey = maxKey)
    val items = generator.take(numItems)
    // TODO: log the number of unique keys and some other properties of the dataset

    val evictionPolicies = Seq(
      new NoPreAggregationEvictionPolicy[Int],
      new OptimalEvictionPolicy[Int](items),
      new RandomEvictionPolicy[Int],
      new RandomEvictionPolicy[Int] with BloomFilterInitialBypass[Int] { override def numEntries = maxKey },
      new CountMinSketchEvictionPolicy[Int](0.01, 1E-3),
      new CountMinSketchEvictionPolicy[Int](0.01, 1E-3) with BloomFilterInitialBypass[Int] { override def numEntries = maxKey }
    )
    val stats = evictionPolicies.map(policy => (policy.toString, simulateCountByKey[Int](items, 100, policy)))
    for ((policyName, numOutputTuples) <- stats.sortBy(_._2)) {
      println("%10d %s".format(numOutputTuples, policyName))
    }
  }
}
