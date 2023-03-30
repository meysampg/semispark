package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

// we need to know which partition is related to which rdd
class UnionPartition[T: ClassTag](rdd: RDD[T], partition: Partition) extends Partition {
  def iterator(): Iterator[T] = rdd.iterator(partition)
}

class UnionRDD[T: ClassTag](r1: RDD[T], r2: RDD[T]) extends RDD[T](r1.sparkContext) {
  @transient val _partitions: Array[UnionPartition[T]] =
    r1.partitions.map(p => new UnionPartition[T](r1, p)) ++ r2.partitions.map(p => new UnionPartition[T](r2, p))

  override var partitions: Array[Partition] = _partitions.asInstanceOf[Array[Partition]]

  // given partition knows about the RDD that the partition is related to that
  override def iterator(partition: Partition): Iterator[T] = partition.asInstanceOf[UnionPartition[T]].iterator()
}
