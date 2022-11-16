package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/15
 */
object CheckPointHDFSDemo {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","root")

    val conf: SparkConf = new SparkConf().setAppName("CheckPointHDFSDemo").setMaster("local")
    val sc = new SparkContext(conf)

    // 通过 SparkContext 对象设置检查点存储文件夹
    sc.setCheckpointDir("hdfs://124.220.74.230:9000/ck")

    val rdd: RDD[(String, Int)] = sc.textFile("spark-core\\data\\wc.txt")
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    // 通过 RDD 设置检查点
    rdd.checkpoint()

    // 检查点需要 action 算子触发
    rdd.saveAsTextFile("hdfs://124.220.74.230:9000/output3")

    println(rdd.getCheckpointFile)
  }
}
