package edu.berkeley.cs.amplab.aggregationsketches.aggregators

import scala.collection.mutable
import scala.collection.immutable.TreeSet

object Oracles {
  /**
   * An oracle that can see the complete key stream in advance.  Implements
   * Belady's Algorithm by evicting the key that will be used furthest
   * in the future
   *
   * @param stream the complete stream of keys
   * @param bufferSize the buffer size, measured in (key, value) pairs.
   * @return the total output size of a pre-aggregator that uses this oracle.
   */
  def completeLookahead[K](stream: Stream[K], bufferSize: Int): Int = {
    // An index over the positions where keys occur
    val nextKeyPositions: Map[K, mutable.Queue[Int]] =
      stream.zipWithIndex.groupBy(_._1).mapValues(x => mutable.Queue.concat(x.map(_._2)))
    // Track the keys currently in the buffer
    val buffer = mutable.HashSet.empty[K]
    // Support efficient lookup of the next key to evict:
    var nextVictims = TreeSet.empty[(Int, K)](Ordering.by(_._1))
    val nextVictimsInverse = mutable.HashMap.empty[K, (Int, K)]

    def getNextVictim(): K = {
      val nextVictimKey = nextVictims.firstKey._2
      nextVictims -= nextVictims.firstKey
      nextVictimKey
    }

    def updateNextVictimsForKey(key: K, newPosition: Int) {
      nextVictimsInverse.get(key).foreach { oldEntry =>
        nextVictims -= oldEntry
      }
      val newEntry = (newPosition, key)
      nextVictimsInverse(key) = newEntry
      nextVictims += newEntry
    }

    var evictedTuples = 0

    for (key <- stream) {
      val nextPositionForKey = nextKeyPositions.get(key).map(_.dequeue()).getOrElse(Int.MaxValue)
      updateNextVictimsForKey(key, nextPositionForKey)
      if (!buffer.contains(key) && buffer.size == bufferSize) {
        val victim = getNextVictim()
        buffer.remove(victim)
        evictedTuples += 1
      }
      updateNextVictimsForKey(key, nextKeyPositions.get(key).map(_.front).getOrElse(Int.MaxValue))
      buffer.add(key)
    }

    evictedTuples + buffer.size
  }

  /**
   * An oracle that knows the complete key frequency distribution,
   * but not the ordering of keys in the stream.  It's eviction policy
   * optimizes for randomized input by only buffering the top `bufferSize` keys.
   *
   * @param stream the complete stream of keys
   * @param bufferSize the buffer size, measured in (key, value) pairs.
   * @return the total output size of a pre-aggregator that uses this oracle.
   */
  def completeFrequencyDistribution[K](stream: Stream[K], bufferSize: Int): Int = {
    val topKeys: Set[K] =
      stream.groupBy(identity).mapValues(_.size).toList.sortBy(_._2).takeRight(bufferSize).map(_._1).toSet
    // We only emit one copy of each of the top keys, plus all copies of other keys:
    stream.filterNot(topKeys.contains).size + topKeys.size
  }
}