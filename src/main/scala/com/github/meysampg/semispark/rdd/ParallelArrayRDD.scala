package com.github.meysampg.semispark.rdd

import com.github.meysampg.semispark.SparkContext

import java.util.concurrent.atomic.AtomicLong
import scala.reflect.ClassTag

class ParallelArrayPartition[T]
(
  val arrayId: Long,
  val slices: Int,
  val values: Seq[T],
) {
  def iterator(): Iterator[T] = values.iterator
}

class ParallelArrayRDD[T: ClassTag]
(
  @transient sc: SparkContext,
  val data: Seq[T],
  val numSlices: Int,
) extends RDD[T, ParallelArrayPartition[T]](sc) {
  private val id = ParallelArrayRDD.newId()

  override var partitions: Array[ParallelArrayPartition[T]] = {
    val slices = ParallelArrayRDD.slice(data, numSlices)
    slices.indices.map(i => new ParallelArrayPartition(id, i, slices(i))).toArray
  }

  override def iterator(split: ParallelArrayPartition[T]): Iterator[T] = split.iterator()
}

private object ParallelArrayRDD {
  private val nextId: AtomicLong = new AtomicLong(0)

  def newId(): Long = nextId.getAndIncrement()

  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    val array = seq.toArray
    (0 until numSlices).map(i => {
      val start = ((i * array.length.toLong) / numSlices).toInt
      val end = (((i + 1) * array.length.toLong) / numSlices).toInt

      array.slice(start, end)
    })
  }
}