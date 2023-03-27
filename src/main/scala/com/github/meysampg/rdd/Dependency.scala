package com.github.meysampg.rdd

abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
  def partitioner: Option[Partitioner]
  def getParents(partitionId: Int): Seq[Int]
}