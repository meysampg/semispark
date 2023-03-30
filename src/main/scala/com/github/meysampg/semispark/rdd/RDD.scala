package com.github.meysampg.semispark.rdd

import com.github.meysampg.semispark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

abstract class RDD[T: ClassTag](@transient sc: SparkContext) {
  var partitions: Array[Partition]

  def sparkContext: SparkContext = sc

  def iterator(partition: Partition): Iterator[T]

  def cache(): RDD[T] = new CachedRDD(this)

  // Transformations
  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD[T, U](this, f)

  def filter(p: T => Boolean): RDD[T] = new FilteredRDD[T](this, p)

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
