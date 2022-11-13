import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/1
 */
object PiDemo {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("PiDemo").setMaster("local[*]")

    var sc = new SparkContext(conf)

    val NUM_SAMPLES=(math.random()*100).toInt

    val count = sc.parallelize(seq= 1 to NUM_SAMPLES).filter{
        _=>
        val x = math.random()
        val y = math.random()
        x*x+y*y < 1
    }.count()

    println(s"Pi is  ${4.0*count/NUM_SAMPLES}")


  }
}
