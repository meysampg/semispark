package examples

import com.github.meysampg.semispark.SparkContext

object SemiSparkSum {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local")

    val rdd1 = sparkContext.parallelize(Range(0, 1000), 10)
    val rdd2 = rdd1.map(_ * 2)
    val rdd3 = rdd1.filter(_ % 2 == 0)
    val result = rdd2.reduce(_ + _)

    println(result)
    println(rdd2.count())
    println(rdd3.count())
  }
}
