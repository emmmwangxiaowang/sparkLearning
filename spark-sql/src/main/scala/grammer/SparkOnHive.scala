package grammer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/21
 */
object SparkOnHive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("SparkOnHive")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    sparkSession.sql("show databases").show()
  }
}
