package com.github.meysampg.semispark.rdd

import scala.reflect.ClassTag

class CollectTask[T: ClassTag](rdd: RDD[T], split: Partition) extends RDDTask[T, Array[T]](rdd, split) {
  override def run: Array[T] = {
    println("Processing CollectTask: " + split)
    rdd.iterator(split).toArray
  }
}
