package edu.berkeley.cs.amplab.aggregationsketches

import scala.collection.mutable
import scala.util.Random
import com.twitter.algebird.CountMinSketchMonoid


/**
 * When processing a particular record, we have to make a series of decisions:
 *
 * - Do we immediately forward the tuple or do we buffer it?
 * - If we buffer it, is there space in the buffer?
 *    - If there isn't space, do we bypass the cache or evict?
 *    - If we evict, how do we select the victim?
 *
 * To make these decisions, we'll have stateful operators that can maintain
 * statistics over the input stream.  These operators can be composed to build
 * new buffer management policies.
 */
trait EvictionPolicy[K] {
  type Buffer = mutable.Map[K, _]

  /** Called when first seeing a key to determine whether it should immediately bypass the cache */
  def shouldBypassCache(key: K): Boolean = false

  // The next three methods are mutually-exclusive: only one of them will be called for a given record:

  /** Called after key is aggregated into an existing cache entry */
  def notifyCacheHit(key: K) {}

  /** Called after a cache miss where we have space to add the element to the cache */
  def notifyCacheMiss(key: K) {}

  /** Called after a cache miss when the cache is full and we may have to evict something.
    *
    * @param key
    * @param buffer
    * @return the victim, or None to bypass the cache.
    */
  def chooseVictim(key: K, buffer: Buffer): Option[K]
}

class NoPreAggregationEvictionPolicy[K] extends EvictionPolicy[K] {
  override def shouldBypassCache(key: K) = true

  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    throw new UnsupportedOperationException()
  }

  override def toString: String = "NoPreaggregationEvictionPolicy"

}

class RandomEvictionPolicy[K](seed: Long = 42) extends EvictionPolicy[K] {
  private val rand = new Random(seed)

  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    val chosenKey = rand.nextInt(buffer.size)
    Some(buffer.keySet.toIndexedSeq(chosenKey))
  }

  override def toString: String = "RandomEvictionPolicy"
}

class CountMinSketchEvictionPolicy[K](eps: Double, delta: Double, seed: Int = 42,
                                      heavyHittersPct: Double = 0.01) extends EvictionPolicy[K] {
  private val CMS = new CountMinSketchMonoid(eps, delta, seed, heavyHittersPct)
  private var sketch = CMS.zero

  private def updateSketch(key: K) {
    sketch ++= CMS.create(key.hashCode())
  }

  override def notifyCacheHit(key: K) {
    updateSketch(key)
  }

  override def notifyCacheMiss(key: K) {
    updateSketch(key)
  }

  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    updateSketch(key)
    // The estimated least frequent key:
    Some(buffer.keysIterator.minBy(key => sketch.frequency(key.hashCode()).estimate))
  }

  override def toString: String = "CountMinSketchEvictionPolicy[eps=" + eps + ", delta=" + delta + "]"
}