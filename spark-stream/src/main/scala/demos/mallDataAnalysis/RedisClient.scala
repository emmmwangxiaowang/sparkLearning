package demos.mallDataAnalysis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.streaming.Seconds
import redis.clients.jedis.{Jedis, JedisPool}

import java.time.Duration


/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/12/1
 */
object RedisClient {

  val redisHost="192.168.254.110"
  val redisPort = 6379
  val redisTimeout=30000
  private val config = new GenericObjectPoolConfig[Jedis]
  config.setEvictorShutdownTimeoutMillis(Duration.ofSeconds(redisTimeout))
  lazy val pool = new JedisPool(config, redisHost)


  lazy val hook: Thread = new Thread {
    override def run(): Unit = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run())

  def main(args: Array[String]): Unit = {
    val dbIndex = 0
    val jedis: Jedis = RedisClient.pool.getResource
    jedis.select(dbIndex)
    jedis.set("test","1")
    println(jedis.get("test"))
    RedisClient.pool.returnResource(jedis)
  }
}
