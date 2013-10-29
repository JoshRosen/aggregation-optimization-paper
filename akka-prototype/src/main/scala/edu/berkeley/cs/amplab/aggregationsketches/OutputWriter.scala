package edu.berkeley.cs.amplab.aggregationsketches

trait OutputWriter[-K, -C] {
  def write(item: (K, C))
  def close()
}

class CountingOutputWriter extends OutputWriter[Any, Any] {
  private var count = 0

  def write(item: (Any, Any)) {
    count += 1
  }

  def close() {}

  def getCount: Int = count

}
