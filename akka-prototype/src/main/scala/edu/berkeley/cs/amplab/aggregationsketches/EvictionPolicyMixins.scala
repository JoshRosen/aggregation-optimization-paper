package edu.berkeley.cs.amplab.aggregationsketches

import com.twitter.algebird.{BloomFilter, BloomFilterMonoid}

trait BloomFilterInitialBypass[K] extends EvictionPolicy[K] {
  val numEntries = 1000
  val fpProb = 0.01
  val seed = 42

  private val width = BloomFilter.optimalWidth(numEntries, fpProb)
  private val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
  private val bfMonoid = new BloomFilterMonoid(numHashes, width, seed)
  private var bf = bfMonoid.zero

  println("width=" + width + "; numHashes=" + numHashes)

  override def shouldBypassCache(key: K): Boolean = {
    super.shouldBypassCache(key) // so the super's stats can be updated
    val keyStr = key.toString
    if (bf.contains(keyStr).not.isTrue) {
      println("Bypassing first sighting of " + keyStr)
      bf += keyStr
      true
    } else {
      println("Aggregating " + keyStr)
      false
    }
  }
}