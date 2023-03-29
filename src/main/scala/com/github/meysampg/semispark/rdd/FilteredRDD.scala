package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class FilteredRDD[T: ClassTag, Split](rdd: RDD[T, Split], p: T => Boolean) extends RDD[T, Split](rdd.sparkContext) {
  override var partitions: Array[Split] = rdd.partitions

  override def iterator(split: Split): Iterator[T] = rdd.iterator(split).filter(p)
}
