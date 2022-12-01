package demos.mallDataAnalysis

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/29
 */
object KafkaMallProducerDemo {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    // 精确一次
    props.put("acks","all")
    props.put("retries","3")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer" )
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true){
      // 随机生成 id
      val id: Int = Random.nextInt(10)
      // 创建订单成交事件
      val event = new JSONObject()
      // 商品 id
      event.put("id",id)
      // 商品成交价格
      event.put("price",Random.nextInt(10000))

      // 发送消息
      producer.send(new ProducerRecord[String,String]("order",event.toJSONString))
      println("Message sent: "+event)
      // 随机暂停一段时间
      Thread.sleep(Random.nextInt(100))
    }
  }
}
