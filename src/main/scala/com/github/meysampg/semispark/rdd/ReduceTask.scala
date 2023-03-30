package com.github.meysampg.semispark.rdd

class ReduceTask[T](rdd: RDD[T], split: Partition, f: (T, T) => T) extends RDDTask[T, Option[T]](rdd, split) {
  override def run: Option[T] = {
    println("Processing ReduceTask: " + split)

    val iter = rdd.iterator(split)
    if (iter.hasNext) Some(iter.reduceLeft(f))
    else None
  }
}
