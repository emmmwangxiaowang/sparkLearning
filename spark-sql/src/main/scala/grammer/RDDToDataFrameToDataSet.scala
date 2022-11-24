package grammer

import bean.Employee
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/20
 */
object RDDToDataFrameToDataSet {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("RDDToDataFrameToDataSet").master("local").getOrCreate()

    val rdd: RDD[Employee] = sparkSession.sparkContext.makeRDD(List(
      new Employee(7832, "jack", "teacher", 7864, "", 5555, 20),
      new Employee(7832, "merry", "science", 7864, "", 64523, 50),
      new Employee(7832, "tom", "engineer", 7864, "", 5423, 30),
      new Employee(7832, "ak47", "teacher", 7864, "", 5423, 20)
    ))

    import sparkSession.implicits._
    // RDD 转换为 DataFrame
    val dataFrame: DataFrame = rdd.toDF()
    // RDD 转换为 Dataset
    val dataSet: Dataset[Employee] = rdd.toDS()

    // DataFrame 转换为 RDD
    val rdd1: RDD[Row] = dataFrame.rdd
    // rdd1.foreach(println)
    rdd1.foreach(row=>{
      // 直接根据坐标获取, 返回的是任意类型
      val name: Any = row.get(1)
      // 根据坐标加上对应的类型, 返回对应类型
      val eName: String = row.getString(1)
      val empno: Int = row.getInt(0)
      println(empno+" "+eName+" "+name)
    })

    // dataSet 转 rdd 其中的数据直接转成 原始数据类型 Employee
    val rdd2: RDD[Employee] = dataSet.rdd

    // dataSet 转 DataFrame
    val frame: DataFrame = dataSet.toDF()
  }
}
