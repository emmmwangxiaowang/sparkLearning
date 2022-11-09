import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/10/28
 */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test")
      .set("spark.driver.host", "124.220.74.230")
      .set("spark.driver.cores", "2")
      .setMaster("spark://172.11.0.5:7077") //spark://127.0.0.1:7077
      .setJars(List("C:\\Users\\Administrator\\Desktop\\fmmall-master\\sparkLearning\\spark-core\\target\\spark-core-1.0-SNAPSHOT.jar")) // maven打的jar包的路径
      .set("spark.driver.allowMultipleContexts", "true")
    var sc = new SparkContext(conf)

    val A = sc.textFile("hdfs://172.11.0.5:9870/wc.txt")
    val iterator = args.iterator
    while (iterator.hasNext) {
      println(iterator.next())
    }


    sc.stop()
  }
}
