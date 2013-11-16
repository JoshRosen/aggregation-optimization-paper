package edu.berkeley.cs.amplab.aggregationsketches

import org.apache.hadoop.io.file.tfile.RandomDistribution
import scala.util.Random

object DataGenerators {
  def zipf(alpha: Double, minKey: Int = 1, maxKey: Int = 100, seed: Int = 42): Stream[Int] = {
    val z = new RandomDistribution.Zipf(new java.util.Random(seed), minKey, maxKey, alpha)
    Stream.continually(z.nextInt)
  }

  def uniform(maxKey: Int, seed: Int = 42): Stream[Int] = {
    val r = new Random(seed)
    Stream.continually(r.nextInt(maxKey))
  }

  /**
   * Distribution where one key is responsible for most of the records and other keys appear only once.
   */
  def heavyHitter(probabilityOfHeavyHitterKey: Float, seed: Int = 42): Stream[Int] = {
    val r = new Random(seed)
    val heavyHitterKey: Int = 1
    var nextUniqueKey: Int = 1
    Stream.continually {
      if (r.nextFloat() < probabilityOfHeavyHitterKey) {
        heavyHitterKey
      } else {
        nextUniqueKey += 1
        nextUniqueKey
      }
    }
  }


}
