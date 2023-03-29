package com.github.meysampg.semispark.scheduler

import com.github.meysampg.semispark.rdd.Task

import scala.reflect.ClassTag

trait Scheduler {
  def start(): Unit

  def waitForRegister(): Unit

  def runTasks[T: ClassTag](tasks: Array[Task[T]]): Array[T]

  def stop(): Unit
}
