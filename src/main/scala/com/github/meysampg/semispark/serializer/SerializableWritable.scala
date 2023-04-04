package com.github.meysampg.semispark.serializer

import org.apache.hadoop.io.{ObjectWritable, Writable}
import org.apache.hadoop.mapred.JobConf

import java.io.{ObjectInputStream, ObjectOutputStream}

class SerializableWritable[T <: Writable](@transient var t: T) {
  def value: T = t

  override def toString: String = t.toString

  def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new JobConf())
    ow.readFields(in)
    t = ow.get().asInstanceOf[T]
  }
}
