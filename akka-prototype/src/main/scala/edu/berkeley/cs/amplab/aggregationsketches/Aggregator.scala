package edu.berkeley.cs.amplab.aggregationsketches

import scala.collection.mutable


class Aggregator[K, V, C](bufferSize: Int,
                          policy: EvictionPolicy[K],
                          writer: OutputWriter[K, C],
                          createCombiner: V => C,
                          mergeValue: (C, V) => C,
                          mergeCombiners: (C, C) => C) {

  private val buffer = new mutable.HashMap[K, C]
  buffer.sizeHint(bufferSize)

  private def insertNewKey(key: K, value: V) {
    buffer(key) = createCombiner(value)
  }

  private def bypassCache(key: K, value: V) {
    writer.write((key, createCombiner(value)))
  }

  def aggregateStream(stream: Iterator[(K, V)]) {
    for ((key, value) <- stream) {
      if (policy.shouldBypassCache(key)) {
        bypassCache(key, value)
      } else if (buffer.contains(key)) {
        policy.notifyCacheHit(key)
        buffer(key) = mergeValue(buffer(key), value)
      } else if (buffer.size < bufferSize) {
        policy.notifyCacheMiss(key)
        insertNewKey(key, value)
      } else {
        policy.chooseVictim(key, buffer) match {
          case Some(victim) =>
            val combiner = buffer.remove(victim).get
            writer.write((victim, combiner))
            insertNewKey(key, value)
          case None =>
            bypassCache(key, value)
        }
      }
    }
  }

  def close() {
    buffer.foreach(writer.write)
    buffer.clear()
    writer.close()
  }

}

