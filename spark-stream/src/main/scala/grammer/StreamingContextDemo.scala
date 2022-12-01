package grammer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/25
 */
object StreamingContextDemo {
  def main(args: Array[String]): Unit = {
    /*
    StreamingContext 初始化 需要两个参数 --> sparkConf 和 batchDuration
    batchDuration: 提交两次作业之间的时间间隔, 每次会提供一个 DStream 将 数据转换为
    batch --> RDD 所以说 SparkStream的计算时间就是每隔多少时间计算一次数据  --> 准实时流
     */
    val conf: SparkConf = new SparkConf()
      .setAppName("StreamingContextDemo")
      .setMaster("local[*]")
    val duration: Duration = Seconds(5)

    val sc = new StreamingContext(conf, duration)

    // 接受网络端数据
    val resDStream: DStream[(String, Int)] = sc.socketTextStream("124.220.74.230", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    resDStream.print()

    // 开启流式计算
    sc.start()
    // 为了不至于 start 启动程序结束, 必须调用 awaitTermination 方法等待程序业务完成
    // 调用 stop 方法结束程序或异常
    sc.awaitTermination()

  }
}
