package edu.berkeley.cs.amplab.aggregationsketches

object AggregationSketches {

  def simulateCountByKey[K](stream: Iterator[K], bufferSize: Int, policy: EvictionPolicy[K]) {
    val writer = new CountingOutputWriter
    val aggregator = new Aggregator[K, Int, Int](bufferSize, policy, writer, x => x, _ + _, _ + _)
    aggregator.aggregateStream(stream.map(x => (x, 1)))
    aggregator.close()
    println("Sent " + writer.getCount + " tuples!")
  }

  def main(args: Array[String]) {
    val items = DataGenerators.zipf(1.3).take(100)
    println("Items are " + items.toSeq)
    val exactCounts = items.groupBy(x => x).mapValues(_.size)
    println("Top items are " + exactCounts.toSeq)

    simulateCountByKey[Int](items.iterator, 10, new RandomEvictionPolicy[Int] with BloomFilterInitialBypass[Int])
  }
}
