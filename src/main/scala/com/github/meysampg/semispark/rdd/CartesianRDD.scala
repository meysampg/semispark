package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class CartesianPartition(val p1: Partition, val p2: Partition) extends Partition

class CartesianRDD[T: ClassTag, U: ClassTag](r1: RDD[T], r2: RDD[U]) extends RDD[(T, U)](r1.sparkContext) {
  @transient private val _partitions =
    r1.partitions.flatMap(p1 => r2.partitions.map(p2 => new CartesianPartition(p1, p2)))

  override var partitions: Array[Partition] = _partitions.asInstanceOf[Array[Partition]]

  override def iterator(partition: Partition): Iterator[(T, U)] = {
    val cSplits = partition.asInstanceOf[CartesianPartition]
    for (
      p1 <- r1.iterator(cSplits.p1);
      p2 <- r2.iterator(cSplits.p2)
    ) yield (p1, p2)
  }
}
