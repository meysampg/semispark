package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class FilteredRDD[T: ClassTag](rdd: RDD[T], p: T => Boolean) extends RDD[T](rdd.sparkContext) {
  override var partitions: Array[Partition] = rdd.partitions

  override def iterator(split: Partition): Iterator[T] = {
    println("Processing FilteredTask: " + split)
    rdd.iterator(split).filter(p)
  }
}
