package edu.berkeley.cs.amplab.aggregationsketches

import org.apache.hadoop.io.file.tfile.RandomDistribution

object AggregationSketches {

  def generateZipfSamples(numSamples: Int, alpha: Double, minKey: Int = 1, maxKey: Int = 100): Iterator[Int] = {
    val zipf = new RandomDistribution.Zipf(new java.util.Random, minKey, maxKey, alpha)
    (1 to numSamples).iterator.map(_ => zipf.nextInt())
  }

  def simulateCountByKey[K](stream: Iterator[K], bufferSize: Int, policy: EvictionPolicy[K]) {
    val writer = new CountingOutputWriter
    val aggregator = new Aggregator[K, Int, Int](bufferSize, policy, writer, x => x, _ + _, _ + _)
    aggregator.aggregateStream(stream.map(x => (x, 1)))
    aggregator.close()
    println("Sent " + writer.getCount + " tuples!")
  }

  def main(args: Array[String]) {
    val items = generateZipfSamples(100, 1.3).toArray
    println("Items are " + items.toSeq)
    val exactCounts = items.groupBy(x => x).mapValues(_.size)
    println("Top items are " + exactCounts.toSeq)

    simulateCountByKey[Int](items.iterator, 10, new RandomEvictionPolicy[Int] with BloomFilterInitialBypass[Int])
  }
}
