package edu.berkeley.cs.amplab.aggregationsketches

import com.google.common.hash.{Funnels, BloomFilter}

trait BloomFilterInitialBypass[K] extends EvictionPolicy[K] {
  def numEntries: Int = 1000
  val fpProb = 0.01

  private val bf = BloomFilter.create(Funnels.longFunnel(), numEntries, fpProb)

  override def shouldBypassCache(key: K): Boolean = {
    super.shouldBypassCache(key) // so the super's stats can be updated
    bf.put(key.hashCode()) // Guaranteed to return true for first insertion of item
  }

  override def toString: String =
    "BloomFilterInitialBypass[%s, numItems=%d, fpProb=%f]".format(super.toString, numEntries, fpProb)
}