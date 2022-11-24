package grammer

import bean.LocationInfos
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.util.Properties

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/20
 */
object DataRead {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("DataRead").master("local").getOrCreate()

    // 第一种, 通过 format 指定文件类型
    val dataFrame1: DataFrame = sparkSession.read.format("json").load("spark-sql/data/emp.json")
    dataFrame1.show()
    // 第二种, 通过 .xxx 指定文件类型
    val dataFrame2: DataFrame = sparkSession.read.csv("spark-sql/data/info.csv")
    dataFrame2.show()
    // 读取 数据库中的数据
    var properties = new Properties()
    properties.put("user","root")
    properties.put("password","123456")
    val dataFrame3: DataFrame = sparkSession.read.jdbc("jdbc:mysql://124.220.74.230:3306/spark", "location_infos", properties = properties)
    dataFrame3.show()

    // 将 读取的数据更改后写回 数据库
    val rdd: RDD[Row] = dataFrame3.rdd
    val rdd2db: RDD[LocationInfos] = rdd.map(row => {
      (LocationInfos((Integer.parseInt(row.get(0).toString) + 10), row.get(1).toString, Integer.parseInt(row.get(2).toString)))
    })
    import sparkSession.implicits._
    val dataFrame4: DataFrame = rdd2db.toDF()
    dataFrame4.write.mode(saveMode = SaveMode.Overwrite).json("spark-sql/data/db/")
    dataFrame4.write.mode(saveMode = SaveMode.Append).jdbc("jdbc:mysql://124.220.74.230:3306/spark", "location_infos",connectionProperties = properties)

    sparkSession.stop()
  }

}
