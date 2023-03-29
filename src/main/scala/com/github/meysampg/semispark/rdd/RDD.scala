package com.github.meysampg.semispark.rdd

import com.github.meysampg.semispark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

abstract class RDD[T: ClassTag, Split](@transient sc: SparkContext) {
  var partitions: Array[Split]

  def sparkContext: SparkContext = sc

  def iterator(split: Split): Iterator[T]

  // Transformations
  def map[U: ClassTag](f: T => U): RDD[U, Split] = new MappedRDD[T, U, Split](this, f)

  def filter(p: T => Boolean): RDD[T, Split] = new FilteredRDD[T, Split](this, p)

  // Actions
  def count(): Long = map(x => 1L).reduce(_ + _)

  def collect(): Array[T] = {
    val tasks = partitions.map(p => new CollectTask(this, p))
    val results = sc.runTaskObjects(tasks)

    Array.concat(results: _*)
  }

  def reduce(f: (T, T) => T): T = {
    val tasks = partitions.map(p => new ReduceTask(this, p, f))
    val results = new ArrayBuffer[T]
    for (option <- sc.runTaskObjects(tasks); result <- option) {
      results += result
    }

    results.reduceLeft(f)
  }
}
