package edu.berkeley.cs.amplab.aggregationsketches.aggregators


/**
 * Routes tuples to one of two aggregators depending on the value of a predicate.
 */
abstract class PredicateRouter[K, V](onTrue: Aggregator[K, V], onFalse: Aggregator[K, V]) extends Aggregator[K, V] {
  def predicate(tuple: (K, V)): Boolean

  final def send(tuple: (K, V)) {
    if (predicate(tuple)) {
      onTrue.send(tuple)
    } else {
      onFalse.send(tuple)
    }
  }

  final def flush() {
    onTrue.flush()
    onFalse.flush()
  }

  final def close() {
    onTrue.flush()
    onFalse.flush()
    onTrue.close()
    onFalse.close()
  }

}
