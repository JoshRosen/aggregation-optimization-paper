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

  override def toString: String = getClass.getSimpleName
}

class NoPreAggregationEvictionPolicy[K] extends EvictionPolicy[K] {
  override def shouldBypassCache(key: K) = true

  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    throw new UnsupportedOperationException()
  }
}

class RandomEvictionPolicy[K](seed: Long = 42) extends EvictionPolicy[K] {
  private val rand = new Random(seed)

  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    val chosenKey = rand.nextInt(buffer.size)
    Some(buffer.keySet.toIndexedSeq(chosenKey))
  }

  override def toString: String = "RandomEvictionPolicy"
}

class FIFOEvictionPolicy[K] extends EvictionPolicy[K] {
  private val queue = new mutable.Queue[K]()

  override def notifyCacheMiss(key: K) {
    queue += key
  }
  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    queue += key
    val victim = queue.dequeue()
    assert(buffer.contains(victim))
    Some(victim)
  }

  override def toString: String = "FIFOEvictionPolicy"
}

class LRUEvictionPolicy[K](bufferSize: Int) extends EvictionPolicy[K] {
  private val map = new java.util.LinkedHashMap[K, Int](bufferSize, 0.75f, true)

  override def notifyCacheMiss(key: K) {
    map.put(key, 1)
  }

  override def notifyCacheHit(key: K) {
    map.put(key, 1)
  }

  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    map.put(key, 1)
    val iterator = map.entrySet().iterator()
    val victim = iterator.next().getKey
    iterator.remove()
    assert(buffer.contains(victim))
    Some(victim)
  }

  override def toString: String = "LRUEvictionPolicy"
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
    val leastFrequentKey = buffer.keysIterator.minBy(k => sketch.frequency(k.hashCode()).estimate)
    val minKeyFrequency = sketch.frequency(leastFrequentKey.hashCode())
    val newKeyFrequency = sketch.frequency(key.hashCode())
    if (newKeyFrequency.estimate >= minKeyFrequency.estimate) {
      Some(leastFrequentKey)
    } else {
      None
    }
  }

  override def toString: String = "CountMinSketchEvictionPolicy[eps=" + eps + ", delta=" + delta + "]"
}

/**
 * Makes optimal eviction decisions, based on Belady's Algorithm.
 * This technique isn't feasible in practice, because it requires lookahead over the entire stream,
 * but it serves as a useful lower bound in benchmarks.
 */
class OptimalEvictionPolicy[K](stream: Seq[K]) extends EvictionPolicy[K] {
  private var streamTail = stream

  override def notifyCacheHit(key: K) {
    streamTail = streamTail.drop(1)
  }

  override def notifyCacheMiss(key: K) {
    streamTail = streamTail.drop(1)
  }

  override def chooseVictim(key: K, buffer: Buffer): Option[K] = {
    streamTail = streamTail.drop(1)
    // Evict the key whose next appearance is furthest in the future:
    def nextUse(key: K) = if (streamTail.contains(key)) {
      streamTail.indexOf(key)
    } else {
      Int.MaxValue
    }
    Some(buffer.keysIterator.maxBy(nextUse))
  }
}