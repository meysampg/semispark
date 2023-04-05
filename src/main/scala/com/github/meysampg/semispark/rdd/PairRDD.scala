package com.github.meysampg.semispark.rdd

import scala.collection.mutable
import scala.reflect.ClassTag

// This class doesn't extend RDD explicitly, but all operations related to RDD[(K, V)] implicitly
// could be delegated to functions of this class.
class PairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
  // actions
  def reduceByKey(f: (V, V) => V): Map[K, V] = {
    def merge(m1: mutable.HashMap[K, V], m2: mutable.HashMap[K, V]): mutable.HashMap[K, V] = {
      for ((k, v) <- m1) {
        m2.get(k) match {
          case None => m2(k) = v
          case Some(w) => m2(k) = f(v, w)
        }
      }

      m2
    }

    rdd.map(mutable.HashMap(_)).reduce(merge).toMap
  }
}
