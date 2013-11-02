package edu.berkeley.cs.amplab.aggregationsketches.aggregators


trait Aggregator[-K, -V] {
  def flush()
  def close()
  def send(tuple: (K, V))
  def memoryUsageInBytes: Long
}