package com.github.meysampg.semispark.rdd

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.MapMaker

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.reflect.ClassTag

class CachedRDD[T: ClassTag](rdd: RDD[T]) extends RDD[T](rdd.sparkContext) {
  private val id: Long = CachedRdd.getId()

  override var partitions: Array[Partition] = rdd.partitions

  override def iterator(split: Partition): Iterator[T] = {
    val key = id + "::" + split.toString
    val cache = CachedRdd.cache
    val loading = CachedRdd.loading
    val cachedValue = cache.getIfPresent(key)

    if (cachedValue != null) {
      // already we have a cached value, return it
      Iterator.from(cachedValue.asInstanceOf[Array[T]])
    } else {
      // we don't have a cached value
      loading.synchronized {
        if (loading.contains(key)) {
          // some other RDD is trying to cache value with this key
          while (loading.contains(key)) {
            // wait until that RDD cache values
            try {
              loading.wait()
            } catch {
              case _ =>
            }
          }
          // read the cached value from that RDD
          return Iterator.from(cachedValue.asInstanceOf[Array[T]])
        } else {
          // no one is currently trying to cache RDD with this key, acquire the lock
          loading.add(key)
        }
      }

      // no return has happened, so we have to cache our RDD
      val toBeCachedValue = rdd.iterator(split).toArray
      cache.put(key, toBeCachedValue)

      // signal to others to fetch cached result if they are waiting
      loading.synchronized {
        loading.remove(key)
        loading.notifyAll()
      }

      Iterator.from(toBeCachedValue)
    }
  }
}

private object CachedRdd {
  val id = new AtomicLong(0)

  def getId(): Long = id.getAndIncrement()

  val cache: Cache[String, AnyRef] = CacheBuilder.newBuilder().softValues().build[String, AnyRef]()

  val loading: mutable.HashSet[String] = new mutable.HashSet[String]()
}