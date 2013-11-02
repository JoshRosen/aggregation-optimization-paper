package edu.berkeley.cs.amplab.aggregationsketches.aggregators

import java.util
import scala.collection.mutable

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

  /** Walks the plan, in DFS order, from the root to the sink, yielding each node exactly once */
  def walkPlan: Iterator[Aggregator[K, V]] = {
    val visited = new util.IdentityHashMap[Aggregator[K, V], Boolean]
    val nodes = new mutable.ArrayBuffer[Aggregator[K, V]]()
    def visit(node: Aggregator[K, V]) {
      if (!visited.containsKey(node)) {
        nodes += node
        visited.put(node, true)
        node match {
          case bpa: BufferingPreAggregator[K, V] =>
            visit(bpa.nextAgg)
          case predRouter: PredicateRouter[K, V] =>
            visit(predRouter.onTrue)
            visit(predRouter.onFalse)
          case _ =>
            // Do nothing
        }
      }
    }
    visit(rootAggregator)
    nodes.iterator
  }

  def memoryUsageInBytes: Long = {
    walkPlan.map(_.memoryUsageInBytes).sum
  }
}
