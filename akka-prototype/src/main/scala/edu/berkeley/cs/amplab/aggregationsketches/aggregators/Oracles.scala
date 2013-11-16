package edu.berkeley.cs.amplab.aggregationsketches.aggregators

import scalaz._
import Scalaz._

import scala.collection.mutable

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
    val buffer = new mutable.HashSet[K]()
    var outputSize: Int = 0
    var z = stream.toZipper.get
    // ^ TODO: there's got to be a way to do this without mutability,
    // but my function programming skills are a bit too rusty for that
    // right now; I'll come back to it later when I have more time.
    while (!z.rights.isEmpty) {
      val key = z.focus
      val future = z.rights
      if (!buffer.contains(key)) {
        assert(buffer.size <= bufferSize)
        if (buffer.size == bufferSize) {
          // Evict the key whose next appearance is furthest in the future:
          def nextUse(key: K) = if (future.contains(key)) {
            future.indexOf(key)
          } else {
            Int.MaxValue
          }
          buffer.remove(buffer.maxBy(nextUse))
          outputSize += 1
        }
        buffer.add(key)
      }
      z = z.next.get
    }
    outputSize += buffer.size
    outputSize
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