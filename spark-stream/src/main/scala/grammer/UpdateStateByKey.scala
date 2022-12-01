package grammer

import org.apache.log4j.{Level, LogManager, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/29
 */
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {

   val conf: SparkConf = new SparkConf()
      .setAppName("UpdateStateByKey")
      .setMaster("local[*]")

    val sc = new StreamingContext(conf, Seconds(3))

    sc.sparkContext.setLogLevel("WARN")

    sc.checkpoint("spark-stream/data/checkPoint/demo1/")

    sc.socketTextStream("124.220.74.230", 9999)
      .flatMap(line => {
        line.split(" ")
      })
      .map((_, 1))
      .updateStateByKey(updateFunc)
      .print()

    sc.start()
    sc.awaitTermination()

  }

  def updateFunc(seq: Seq[Int],option: Option[Int]): Option[Int] ={
    Option(seq.sum+option.getOrElse(0))
  }
}
