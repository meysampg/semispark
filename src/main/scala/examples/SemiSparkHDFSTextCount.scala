package examples

import com.github.meysampg.semispark.SparkContext
import com.github.meysampg.semispark.rdd.RDD

object SemiSparkHDFSTextCount {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[4]")

    val text: RDD[String] = sparkContext.textFile("hdfs://localhost:9000/semi/spark.txt")
    val spaceSplit: RDD[String] = text.flatMap(_.split(" ").iterator)
    val wordSet: RDD[(String, Int)] = spaceSplit.groupBy(s => s).map(t => (t._1, t._2.size)).cache()

    println(wordSet.count())
    println(wordSet.take(5).mkString("Array(", ", ", ")"))
  }
}
