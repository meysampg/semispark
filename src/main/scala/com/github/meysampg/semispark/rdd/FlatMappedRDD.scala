package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class FlatMappedRDD[T: ClassTag, U: ClassTag](rdd: RDD[T], f: T => Iterator[U]) extends RDD[U](rdd.sparkContext) {
  override var partitions: Array[Partition] = rdd.partitions

  override def iterator(partition: Partition): Iterator[U] = {
    println("Processing FlatMappedRDD: " + partition)
    rdd.iterator(partition).flatMap(f)
  }
}
