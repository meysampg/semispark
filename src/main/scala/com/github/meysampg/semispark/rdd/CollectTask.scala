package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class CollectTask[T: ClassTag, Split](rdd: RDD[T, Split], split: Split) extends RDDTask[T, Array[T], Split](rdd, split) {
  override def run: Array[T] = rdd.iterator(split).toArray
}
