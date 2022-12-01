package demos.mallDataAnalysis

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/29
 */
object OrderConsumer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderConsumer").setMaster("local[*]")
    val sc = new StreamingContext(conf, Seconds(1))

    // 提供 kafka 配置
    val kafka: Map[String, Object] =Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hadoop01:9092,hadoop022:9092,hadoop03:9092",
      ConsumerConfig.GROUP_ID_CONFIG->"wang",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 读取 kafka 中的数据信息 生成 DStream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(sc,
      // 本地化策略: 将 kafka 的分区数据均匀的分配到各个执行的 Executor 中
      LocationStrategies.PreferConsistent,
      // 表示要从使用 kafka 进行消费 [offset 谁来管理, 从那个位置开始消费数据]
      ConsumerStrategies.Subscribe[String, String](Set("order"), kafka)
    )

    val events: DStream[JSONObject] = kafkaStream.flatMap(line => Some(JSON.parseObject(line.value())))

    val orders: DStream[(String, Int, Long)] = events
      .map(x => (x.getString("id"), x.getLong("price")))
      .groupByKey()
      .map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))



    // DStream 操作的是一个个流(RDD) 这个 foreachRDD 就是对每个批次的流进行操作
    orders.foreachRDD(x=>{
      // 对 RDD 的每个 partition 进行操作
      x.foreachPartition(partition=>{
        // 对 partition 中的具体数据进行操作
        partition.foreach(x=>{
          println("id"+x._1+" count="+ x._2 +" price="+x._3)

          // 保存到 redis 中
          val jedis: Jedis = RedisClient.pool.getResource
          jedis.select(1)
          // 每个商品销售额累加
          jedis.hincrBy("orderTotalKey",x._1,x._3)
          // 上一分钟每个商品销售额
          jedis.hset("oneMinTotalKey",x._1,x._3.toString)
          // 总销售额累加
          jedis.incrBy("totalKey",x._3)
          // 释放资源
          RedisClient.pool.returnResource(jedis)
        })

      })
    })

    sc.start()
    sc.awaitTermination()

  }
}
