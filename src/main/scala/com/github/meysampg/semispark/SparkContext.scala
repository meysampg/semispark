package com.github.meysampg.semispark

import com.github.meysampg.semispark.rdd.{HDFSTextFileRDD, ParallelArrayRDD, RDD, Task}
import com.github.meysampg.semispark.scheduler.{LocalScheduler, Scheduler}

import scala.reflect.ClassTag
import scala.util.matching.Regex

class SparkContext(master: String) {
  private val localRegex: Regex = """local\[([0-9]+)\]""".r

  val scheduler: Scheduler = master match {
    case "local" => new LocalScheduler(1)
    case localRegex(threads) => new LocalScheduler(threads.toInt)
    case _ => throw new UnsupportedOperationException("unsupported scheduler")
  }

  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int): RDD[T] =
    new ParallelArrayRDD[T](this, seq, numSlices)

  def textFile(path: String): RDD[String] = new HDFSTextFileRDD(this, path)

  def runTaskObjects[T: ClassTag](tasks: Seq[Task[T]]): Array[T] = {
    scheduler.runTasks(tasks.toArray)
  }
}
