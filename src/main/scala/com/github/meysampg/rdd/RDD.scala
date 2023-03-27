package com.github.meysampg.rdd

class RDD[T]
(
  val id: Int,
  val partitions: Array[Partition],
  val dependencies: List[Dependency[_]],
) {
  // Transformations
  def map[U](f: T => U): RDD[U] = ???

  def filter(p: T => Boolean): RDD[T] = ???

  def flatMap[U](f: T => RDD[U]): RDD[U] = ???

  def groupBy[K](f: T => K): RDD[(K, Iterable[T])] = ???

  // Actions
  def count(): Long = ???

  def collect(): Iterable[T] = ???

  def reduce(f: (T, T) => T): T = ???

  def take(n: Long): Iterable[T] = ???
}
