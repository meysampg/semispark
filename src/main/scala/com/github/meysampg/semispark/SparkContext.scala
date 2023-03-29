package com.github.meysampg.semispark

import com.github.meysampg.semispark.rdd.{ParallelArrayRDD, Task}
import com.github.meysampg.semispark.scheduler.{LocalScheduler, Scheduler}

import scala.reflect.ClassTag
import scala.util.matching.Regex

class SparkContext(master: String) {
  private val localRegex: Regex = """local\[([0-9]+)\]""".r

  private val scheduler: Scheduler = master match {
    case "local" => new LocalScheduler(1)
    case localRegex(threads) => new LocalScheduler(threads.toInt)
    case _ => throw new UnsupportedOperationException("unsupported scheduler")
  }

  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int): ParallelArrayRDD[T] =
    new ParallelArrayRDD[T](this, seq, numSlices)

  def runTaskObjects[T: ClassTag](tasks: Seq[Task[T]]): Array[T] = {
    scheduler.runTasks(tasks.toArray)
  }
}
