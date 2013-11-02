package edu.berkeley.cs.amplab.aggregationsketches.aggregators

class CountAggregator extends Aggregator[Any, Any] {
  private var count = 0

  def getCount = count

  def send(tuple: (Any, Any)) {
    count += 1
  }

  def flush() {}

  def close() {}

}
