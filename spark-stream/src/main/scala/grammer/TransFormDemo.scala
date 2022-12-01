package grammer

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/25
 */
object TransFormDemo {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
      .setAppName("TransFormDemo")
      .setMaster("local[*]")
    val sc = new StreamingContext(conf, Seconds(5))
    sc.sparkContext.setLogLevel("WARN")


    val blacks = List("wang", "li")
    val blackRDD: RDD[(String, Boolean)] = sc.sparkContext.makeRDD(blacks)
      .map((_, true))



    val textStream: ReceiverInputDStream[String] = sc.socketTextStream("124.220.74.230", 9999)
//    textStream.map(x=>x.split(" ")(0))
//      .transform((rdd: RDD[String]) =>{
//        rdd.map(x=>x)
//      })
    val log: DStream[String] = textStream.map(x => (x.split(" ")(0), x))
      .transform((rdd: RDD[(String, String)]) => {
        // RDD[(String, (String, Option[Boolean]))]  联表后返回的数据 第一个参数 是 联表的键, 第二个参数是一个二元组,key是 值, value 是右表中是否存在这个值
        val fulldata: RDD[(String, (String, Option[Boolean]))] = rdd.leftOuterJoin(blackRDD)
        fulldata
          .collect()
          .foreach(println)
        fulldata
          // 过滤掉 黑名单中的数据 若 联表后 存在黑名单中的数据,则 Option 返回的为 false
          .filter(x => x._2._2.getOrElse(false) != true)
          .map(x => {
            x._2._1
          })
      })
    log.print()

    sc.start()

    sc.awaitTermination()
  }
}
