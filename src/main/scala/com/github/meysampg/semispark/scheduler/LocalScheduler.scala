package com.github.meysampg.semispark.scheduler

import com.github.meysampg.semispark.rdd.{Task, TaskResult}

import java.util.concurrent._
import scala.reflect.ClassTag

class LocalScheduler(threads: Int) extends Scheduler {
  private val threadPool: ExecutorService = Executors.newFixedThreadPool(threads, DaemonThreadFactory)

  override def start(): Unit = {}

  override def numCores(): Int = threads

  override def waitForRegister(): Unit = {}

  override def runTasks[T: ClassTag](tasks: Array[Task[T]]): Array[T] = {
    val futures = new Array[Future[TaskResult[T]]](tasks.length)

    for (i <- tasks.indices) {
      futures(i) = threadPool.submit(new Callable[TaskResult[T]]() {
        override def call(): TaskResult[T] = {
          val value = tasks(i).run

          new TaskResult[T](value)
        }
      })
    }

    futures.map(_.get.value)
  }

  override def stop(): Unit = {}
}

object DaemonThreadFactory extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r)
    t.setDaemon(true)
    t
  }
}