package com.github.meysampg.semispark.rdd

abstract class RDDTask[T, U](val rdd: RDD[T], val split: Partition) extends Task[U]
