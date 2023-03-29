package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class MappedRDD[T: ClassTag, U: ClassTag, Split](rdd: RDD[T, Split], f: T => U) extends RDD[U, Split](rdd.sparkContext) {
  override var partitions: Array[Split] = rdd.partitions

  override def iterator(split: Split): Iterator[U] = rdd.iterator(split).map(f)
}
