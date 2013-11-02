package edu.berkeley.cs.amplab.aggregationsketches.aggregators

import com.google.common.hash.{Funnels, BloomFilter}

/**
 *
 * Uses a Bloom filter to perform different processing for keys that we have seen for the first time.
 * This may offer benefits when aggregating over a stream with a long tail of unique keys.
 *
 * @param numEntries the number of expected unique keys inserted into the Bloom filter.
 * @param fpProb the desired false positive probability (must be positive and less than 1.0)
 * @param resetThreshold threshold on the maximum false positive probability before we clear and reset the Bloom filter.
 *
 * @param onMatch aggregator for keys that hit the Bloom filter (i.e. keys that we've may have seen before)
 * @param onMiss aggregator for keys that miss the Bloom filter (i.e. keys that we definitely haven't seen)
x */
class BloomFilterRouter[K, V](numEntries: Int, fpProb: Double, resetThreshold: Float = 1.0f)
                                   (onMatch: Aggregator[K, V], onMiss: Aggregator[K, V])
  extends PredicateRouter[K, V](onMatch, onMiss) {

  private var bf = BloomFilter.create(Funnels.longFunnel(), numEntries, fpProb)

  override def predicate(tuple: (K, V)): Boolean = {
    if (bf.expectedFpp() > 0.5) {
      bf = BloomFilter.create(Funnels.longFunnel(), numEntries, fpProb)
    }
    !bf.put(tuple._1.hashCode()) // (put() is guaranteed to return true for the first insertion of an item)
  }

  override val memoryUsageInBytes: Long = {
    val optimalNumOfBits = bf.getClass.getDeclaredMethod("optimalNumOfBits", java.lang.Long.TYPE, java.lang.Double.TYPE)
    optimalNumOfBits.setAccessible(true)
    val numBits =
      optimalNumOfBits.invoke(bf, new java.lang.Long(numEntries), new java.lang.Double(fpProb)).asInstanceOf[Long]
    optimalNumOfBits.setAccessible(false)
    scala.math.round(numBits / 8.0)
  }
}
