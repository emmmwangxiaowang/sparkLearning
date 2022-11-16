package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/15
 */
object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CheckPointDemo").setMaster("local")
    val sc = new SparkContext(conf)

    // 通过 SparkContext 对象设置检查点存储文件夹
    sc.setCheckpointDir("ck")

    val rdd: RDD[(String, Int)] = sc.textFile("spark-core\\data\\wc.txt")
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    // 通过 RDD 设置检查点
    rdd.checkpoint()

    // 检查点需要 action 算子触发
    rdd.saveAsTextFile("cp")

    println(rdd.getCheckpointFile)
  }
}
