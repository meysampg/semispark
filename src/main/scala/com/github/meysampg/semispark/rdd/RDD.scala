package com.github.meysampg.semispark.rdd

import com.github.meysampg.semispark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class RDD[T: ClassTag](@transient sc: SparkContext) {
  var partitions: Array[Partition]

  def sparkContext: SparkContext = sc

  def iterator(partition: Partition): Iterator[T]

  def cache(): RDD[T] = new CachedRDD(this)

  // Transformations
  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD[T, U](this, f)

  def flatMap[U: ClassTag](f: T => Iterator[U]): RDD[U] = new FlatMappedRDD[T, U](this, f)

  def filter(p: T => Boolean): RDD[T] = new FilteredRDD[T](this, p)

  def groupBy[K: ClassTag](f: T => K): RDD[(K, Iterator[T])] = new GroupedByRDD(this, f)

  def cartesian[U: ClassTag](that: RDD[U]): RDD[(T, U)] = new CartesianRDD[T, U](this, that)

  def union(that: RDD[T]): RDD[T] = new UnionRDD[T](this, that)

  // Actions
  def count(): Long = map(x => 1L).reduce(_ + _)

  def collect(): Array[T] = {
    val tasks = partitions.map(p => new CollectTask(this, p))
    val results = sc.runTaskObjects(tasks)

    Array.concat(results: _*)
  }

  def take(n: Int): Array[T] = {
    println(f"Processing take($n)")
    if (n == 0) Array[T]()
    else {
      val buffer = ArrayBuffer[T]()
      for (partition <- partitions; row <- iterator(partition)) {
        buffer += row
        if (buffer.size == n) return buffer.toArray
      }
      buffer.toArray
    }
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

object RDD {
  implicit def rddToPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): PairRDD[K, V] =
    new PairRDD(rdd)
}
