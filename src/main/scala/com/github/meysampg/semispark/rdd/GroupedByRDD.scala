package com.github.meysampg.semispark.rdd

class GroupedByRDD[T, K](rdd: RDD[T], f: T => K) extends RDD[(K, Iterator[T])](rdd.sparkContext) {
  override var partitions: Array[Partition] = rdd.partitions

  override def iterator(partition: Partition): Iterator[(K, Iterator[T])] =
    rdd.map(r => (f(r), r))
      .iterator(partition)
      .to(LazyList)
      .groupBy(_._1)
      .map(t => (t._1, t._2.map(_._2).iterator))
      .iterator
}
