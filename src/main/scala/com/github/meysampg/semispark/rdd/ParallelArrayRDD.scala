package com.github.meysampg.semispark.rdd

import com.github.meysampg.semispark.SparkContext

import java.util.concurrent.atomic.AtomicLong
import scala.reflect.ClassTag

class ParallelArrayPartition[T]
(
  val arrayId: Long,
  val slice: Int,
  val values: Seq[T],
) extends Partition {
  def iterator(): Iterator[T] = values.iterator

  override def toString: String = f"ParallelArrayPartition(arrayId $arrayId, slice $slice)"
}

class ParallelArrayRDD[T: ClassTag]
(
  @transient sc: SparkContext,
  val data: Seq[T],
  val numSlices: Int,
) extends RDD[T](sc) {
  private val id = ParallelArrayRDD.newId()

  override var partitions: Array[Partition] = {
    val slices = ParallelArrayRDD.slice(data, numSlices)
    slices.indices.map(i => new ParallelArrayPartition(id, i, slices(i))).toArray
  }

  override def iterator(split: Partition): Iterator[T] =
    split.asInstanceOf[ParallelArrayPartition[T]].iterator()
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