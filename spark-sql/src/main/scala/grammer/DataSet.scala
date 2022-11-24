package grammer

import bean.Employee
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.immutable.HashSet

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/20
 */
object DataSet {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
    .appName("DataSet").master("local").getOrCreate()

    var list=List(
      new Employee(12,"jack","seller",7923,"",4000,20),
      new Employee(12,"jack","seller",7923,"",4000,20),
      new Employee(12,"jack","seller",7923,"",4000,20),
      new Employee(12,"jack","seller",7923,"",4000,20)
    )

    //
    import sparkSession.implicits._
    val ds: Dataset[Employee] = list.toDS()
    ds.printSchema()
    ds.show()

    val list2 = List(1, 2, 3, 4)
    val ds2: Dataset[Int] = list2.toDS()

    ds2.printSchema()
    ds2.show()

    val ints = new HashSet[Int]()
    println(ints)

    sparkSession.stop()
  }
}
