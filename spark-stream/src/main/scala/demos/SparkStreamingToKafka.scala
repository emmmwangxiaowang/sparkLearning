package demos

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/25
 */
object SparkStreamingToKafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingToKafka")
      .setMaster("local[*]")
    val time =Seconds(3)

    val sc = new StreamingContext(conf, time)
    // 提供 kafka 配置
    val kafka=Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hadoop01:9092,hadoop022:9092,hadoop03:9092",
      ConsumerConfig.GROUP_ID_CONFIG->"wang",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 读取 kafka 中的数据信息 生成 DStream
    val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(sc,
      // 本地化策略: 将 kafka 的分区数据均匀的分配到各个执行的 Executor 中
      LocationStrategies.PreferConsistent,
      // 表示要从使用 kafka 进行消费 [offset 谁来管理, 从那个位置开始消费数据]
      ConsumerStrategies.Subscribe[String, String](Set("test"), kafka)
    )
    //  获取 kv
    val line: DStream[String] = value.map(record => record.value())
    // 开始计算操作
    line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    // 开始任务
    sc.start()
    sc.awaitTermination()
  }

}
