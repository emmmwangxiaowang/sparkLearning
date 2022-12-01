package demos

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/25
 */
object KafkaWCDemo {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    // 精确一次
    props.put("acks","all")
    props.put("retries","3")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer" )
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val arr= Array(
      "hello jack",
      "hello tom",
      "hello merry",
      "hello chen",
      "hello wang",
      "hello jiang",
    )

    val random = new Random()
    while (true){
      val message: String = arr(random.nextInt(arr.length))
      producer.send(new ProducerRecord[String,String]("test",message))
    }

  }
}
