package edu.berkeley.cs.amplab.aggregationsketches

import com.google.common.hash.{Funnels, BloomFilter}

/**
 * Uses a Bloom filter to bypass the cache for keys that we have seen for the first time.
 * This may offer benefits when aggregating over a stream with a long tail of unique keys.
 */
trait BloomFilterInitialBypass[K] extends EvictionPolicy[K] {
  def numEntries: Int = 1000
  val fpProb = 0.05

  private val bf = BloomFilter.create(Funnels.longFunnel(), numEntries, fpProb)

  override def shouldBypassCache(key: K): Boolean = {
    super.shouldBypassCache(key) // so the super's stats can be updated
    bf.put(key.hashCode()) // Guaranteed to return true for first insertion of item
  }

  override def toString: String =
    "BloomFilterInitialBypass[%s, numItems=%d, fpProb=%f]".format(super.toString, numEntries, fpProb)
}


// TODO: make this reasoning more formal; we can actually calculate the relevant probabilities:
/**
 * Like BloomFilterInitialBypass, but clears the Bloom filter once the false-positive
 * probability becomes too high.  This offers significant space advantages for streams
 * with huge numbers of unique keys.
 *
 * The probability of a false positive grows with the
 *
 */
trait BloomFilterInitialBypassWithPeriodicReset[K] extends EvictionPolicy[K] {
  def numEntries: Int = 1000
  val fpProb = 0.05

  private var bf = BloomFilter.create(Funnels.longFunnel(), numEntries, fpProb)

  override def shouldBypassCache(key: K): Boolean = {
    if (bf.expectedFpp() > 0.5) {
      bf = BloomFilter.create(Funnels.longFunnel(), numEntries, fpProb)
    }
    super.shouldBypassCache(key) // so the super's stats can be updated
    bf.put(key.hashCode()) // Guaranteed to return true for first insertion of item
  }

  override def toString: String =
    "BloomFilterInitialBypassWithPeriodicReset[%s, numItems=%d, fpProb=%f]".format(super.toString, numEntries, fpProb)
}