package edu.berkeley.cs.amplab.aggregationsketches.aggregators

class AggregationPlan[K, V](
    val name: String,
    val rootAggregator: Aggregator[K, V],
    val sinkAggregator: Aggregator[K, V])
  extends Aggregator[K, V] {

  def send(tuple: (K, V)) {
    rootAggregator.send(tuple)
  }

  def flush() {
    rootAggregator.flush()
  }

  def close() {
    rootAggregator.close()
  }
}
