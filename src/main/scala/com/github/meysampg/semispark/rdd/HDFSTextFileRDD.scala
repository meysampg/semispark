package com.github.meysampg.semispark.rdd

import com.github.meysampg.semispark.SparkContext
import com.github.meysampg.semispark.serializer.SerializableWritable
import org.apache.hadoop.mapred.{FileInputFormat, InputSplit, JobConf, RecordReader, Reporter, TextInputFormat}
import org.apache.hadoop.io.{LongWritable, Text}

import java.io.EOFException

class HDFSTextFilePartition(@transient s: InputSplit) extends Partition {
  val inputSplit: SerializableWritable[InputSplit] = new SerializableWritable[InputSplit](s)
}

class HDFSTextFileRDD
(
  @transient val sc: SparkContext,
  path: String,
) extends RDD[String](sc) {
  @transient private val conf = new JobConf()

  @transient private val inputFormat = new TextInputFormat()

  FileInputFormat.setInputPaths(conf, path)
  inputFormat.configure(conf)

  @transient private val _partitions =
    inputFormat.getSplits(conf, sc.scheduler.numCores()).map(new HDFSTextFilePartition(_))

  override var partitions: Array[Partition] = _partitions.asInstanceOf[Array[Partition]]

  override def iterator(partition_in: Partition): Iterator[String] = new Iterator[String] {
    val _partition: HDFSTextFilePartition = partition_in.asInstanceOf[HDFSTextFilePartition]
    var reader: RecordReader[LongWritable, Text] = null
    val conf: JobConf = new JobConf()
    conf.set("io.file.buffer.size", System.getProperty("spark.buffer.size", "65536"))
    val tif: TextInputFormat = new TextInputFormat()
    tif.configure(conf)
    reader = tif.getRecordReader(_partition.inputSplit.value, conf, Reporter.NULL)
    val lineNum = new LongWritable()
    val text = new Text()
    var gotNext = false
    var finished = false

    override def hasNext: Boolean = {
      // to enable repeated usage of hasNext, we only read once, but work with flags
      if (!gotNext) {
        try {
          // next return true iff the next key/value read into lineNum and text and false on EOF
          finished = !reader.next(lineNum, text)
        } catch {
          case _: EOFException =>
            finished = true
        }
        gotNext = true
      }
      !finished
    }

    override def next(): String = {
      if (!gotNext) finished = !reader.next(lineNum, text)
      if (finished) throw new NoSuchElementException("end of stream")

      gotNext = false
      text.toString
    }
  }
}