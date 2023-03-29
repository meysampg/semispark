package com.github.meysampg.semispark.rdd

class ReduceTask[T, Split](rdd: RDD[T, Split], split: Split, f: (T, T) => T) extends RDDTask[T, Option[T], Split](rdd, split) {
  override def run: Option[T] = {
    val iter = rdd.iterator(split)
    if (iter.hasNext) Some(iter.reduceLeft(f))
    else None
  }
}
