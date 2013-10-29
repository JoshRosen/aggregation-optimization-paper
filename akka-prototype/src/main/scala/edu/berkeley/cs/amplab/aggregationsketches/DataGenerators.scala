package edu.berkeley.cs.amplab.aggregationsketches

import org.apache.hadoop.io.file.tfile.RandomDistribution

object DataGenerators {
  def zipf(alpha: Double, minKey: Int = 1, maxKey: Int = 100, seed: Int = 42): Stream[Int] = {
    val z = new RandomDistribution.Zipf(new java.util.Random(seed), minKey, maxKey, alpha)
    Stream.continually(z.nextInt)
  }
}
