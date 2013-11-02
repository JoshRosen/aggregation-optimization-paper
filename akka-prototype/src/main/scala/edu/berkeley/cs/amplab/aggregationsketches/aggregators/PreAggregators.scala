package edu.berkeley.cs.amplab.aggregationsketches.aggregators

import scala.collection.mutable
import scala.util.Random
import java.util
import com.twitter.algebird.CountMinSketchMonoid

abstract class BufferingPreAggregator[K, V](
                                             bufferSize: Int,
                                             val nextAgg: Aggregator[K, V],
                                             aggFunc: (V, V) => V) extends Aggregator[K, V] {

  protected val buffer: mutable.Map[K, V]

  protected def handleFullBuffer(tuple: (K, V))

  protected final def bufferPair(tuple: (K, V)) {
    val key = tuple._1
    if (buffer.contains(key)) {
      buffer(key) = aggFunc(buffer(key), tuple._2)
    } else {
      buffer(key) = tuple._2
    }
  }

  override def send(tuple: (K, V)) {
    if (buffer.contains(tuple._1) || buffer.size < bufferSize) {
      bufferPair(tuple)
    } else {
      handleFullBuffer(tuple)
    }
  }

  final override def flush() {
    buffer.foreach(nextAgg.send)
    buffer.clear()
  }

  final override def close() {
    flush()
    nextAgg.flush()
    nextAgg.close()
  }

  override def memoryUsageInBytes: Long = {
    // A back-of-the-envelope estimate, from
    // http://java-performance.info/memory-consumption-of-java-data-types-2/
    (32 * 4) * bufferSize
  }
}

class RandomBufferingPreAggregator[K, V](bufferSize: Int, nextAgg: Aggregator[K, V], seed: Long = 42)
                                        (implicit aggFunc: (V, V) => V)
  extends BufferingPreAggregator[K, V](bufferSize, nextAgg, aggFunc) {
  private val rand = new Random(seed)
  override val buffer = new mutable.HashMap[K, V]
  buffer.sizeHint(bufferSize)

  override def handleFullBuffer(tuple: (K, V)) {
    val evictedKey = buffer.keySet.toIndexedSeq(rand.nextInt(buffer.size))
    val evictedValue = buffer.remove(evictedKey).get
    nextAgg.send((evictedKey, evictedValue))
    bufferPair(tuple)
  }
}

protected abstract class LinkedHashMapBufferingPreAggregator[K, V](bufferSize: Int, nextAgg: Aggregator[K, V],
                                                                   val linkedHashMap: util.LinkedHashMap[K, V])
                                                                (implicit aggFunc: (V, V) => V)
  extends BufferingPreAggregator[K, V](bufferSize, nextAgg, aggFunc) {
  import scala.collection.JavaConverters.mapAsScalaMapConverter
  override val buffer: mutable.Map[K, V] = mapAsScalaMapConverter(linkedHashMap).asScala

  override def handleFullBuffer(tuple: (K, V)) {
    val iterator = linkedHashMap.entrySet().iterator()
    val evictedPair = iterator.next()
    val evictedKey = evictedPair.getKey
    val evictedValue = evictedPair.getValue
    iterator.remove()
    nextAgg.send((evictedKey, evictedValue))
    bufferPair(tuple)
  }

  override def memoryUsageInBytes: Long = {
    val SIZEOF_POINTER = 4
    super.memoryUsageInBytes + bufferSize * SIZEOF_POINTER  // Space for the link pointers
  }}

class FIFOBufferingPreAggregator[K, V](bufferSize: Int, nextAgg: Aggregator[K, V])(implicit aggFunc: (V, V) => V)
  extends LinkedHashMapBufferingPreAggregator[K, V](bufferSize, nextAgg,
    new java.util.LinkedHashMap[K, V](bufferSize, 0.75f, false))

class LRUBufferingPreAggregator[K, V](bufferSize: Int, nextAgg: Aggregator[K, V])(implicit aggFunc: (V, V) => V)
  extends LinkedHashMapBufferingPreAggregator[K, V](bufferSize, nextAgg,
    new java.util.LinkedHashMap[K, V](bufferSize, 0.75f, true))

class CountMinSketchBufferingPreAggregator[K, V]
  (eps: Double, delta: Double, seed: Int = 42)
  (bufferSize: Int, nextAgg: Aggregator[K, V])
  (implicit aggFunc: (V, V) => V)
extends BufferingPreAggregator[K, V](bufferSize, nextAgg, aggFunc) {
  protected val buffer = new mutable.HashMap[K, V]

  // Don't care about heavy hitters here; only want CMS's frequency estimation:
  private val CMS = new CountMinSketchMonoid(eps, delta, seed, heavyHittersPct = 0.99)
  private var sketch = CMS.zero

  override def send(tuple: (K, V)) {
    sketch ++= CMS.create(tuple._1.hashCode())
    super.send(tuple)
  }

  override def handleFullBuffer(tuple: (K, V)) {
    val leastFrequentKey = buffer.keysIterator.minBy(k => sketch.frequency(k.hashCode()).estimate)
    val minKeyFrequency = sketch.frequency(leastFrequentKey.hashCode())
    val newKeyFrequency = sketch.frequency(tuple._1.hashCode())
    if (newKeyFrequency.estimate >= minKeyFrequency.estimate) {
      val evictedValue = buffer.remove(leastFrequentKey).get
      nextAgg.send((leastFrequentKey, evictedValue))
      bufferPair(tuple)
    } else {
      nextAgg.send(tuple)
    }
  }

  override def memoryUsageInBytes: Long = {
    val BYTES_IN_LONG = 8
    super.memoryUsageInBytes + (sketch.width * sketch.depth) * BYTES_IN_LONG
  }
}