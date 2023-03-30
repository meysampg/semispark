package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class MappedRDD[T: ClassTag, U: ClassTag](rdd: RDD[T], f: T => U) extends RDD[U](rdd.sparkContext) {
  override var partitions: Array[Partition] = rdd.partitions

  override def iterator(split: Partition): Iterator[U] = {
    println("Processing MappedRDD: " + split)
    rdd.iterator(split).map(f)
  }
}
