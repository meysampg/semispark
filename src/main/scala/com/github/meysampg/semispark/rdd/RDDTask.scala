package com.github.meysampg.semispark.rdd

abstract class RDDTask[T, U, Split](val rdd: RDD[T, Split], val split: Split) extends Task[U]
