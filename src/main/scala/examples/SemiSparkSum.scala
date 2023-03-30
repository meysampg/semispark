package examples

import com.github.meysampg.semispark.SparkContext
import com.github.meysampg.semispark.rdd.RDD

object SemiSparkSum {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[4]")

    val rdd1: RDD[Int] = sparkContext.parallelize(Range(0, 1000), 10)
    val rdd2: RDD[Int] = rdd1.map(_ * 2)
    val rdd3: RDD[Int] = rdd1.filter(_ % 2 == 0)
    val cachedRdd3: RDD[Int] = rdd3.cache()
    val result: Int = rdd2.reduce(_ + _)

    println(result)
    println(rdd2.count())
    println(rdd3.count())
    println(cachedRdd3.count())
    println(cachedRdd3.count())
    println(cachedRdd3.count())
  }
}
