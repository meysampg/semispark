package com.github.meysampg.semispark.rdd

trait Task[T] {
  def run: T
}

class FunctionTask[T](body: () => T) extends Task[T] {
  def run: T = body()
}