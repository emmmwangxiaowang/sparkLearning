package grammer

import bean.Employee
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/20
 */
object DataFrame {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataFrame")
      .master("local[*]").getOrCreate()

    val employees = List({
      ////{"EMPNO":7902,"ENAME":"FORD","JOB":"ANALYST","MGR":7566,"HIREDATE":"2011-07-02T22:12:13.000+08:00","SAL":3000,"DEPTNO":20}
      new Employee(7923, "HUNT", "ANALYST", 7536, "2011-07-02T22:12:13.000+08:00", 4000, 10)
      new Employee(7965, "HUNT", "ANALYST", 7523, "2011-07-02T22:12:13.000+08:00", 4000, 20)
      new Employee(7435, "HUNT", "ANALYST", 7523, "2011-07-02T22:12:13.000+08:00", 5000, 20)
      new Employee(7754, "HUNT", "ANALYST", 7536, "2011-07-02T22:12:13.000+08:00", 3000, 10)
      new Employee(7534, "HUNT", "ANALYST", 7432, "2011-07-02T22:12:13.000+08:00", 7000, 30)
    })

    import sparkSession.implicits._
    val dataFrame = employees.toDF()
    dataFrame.printSchema()
    dataFrame.show()

    // 通过 sparkContext 构建 RDD
    val rows = sparkSession.sparkContext.makeRDD(List(
      // ROW 表示一条数据
      Row(7923, "HUNT", "ANALYST", 7536, "2011-07-02T22:12:13.000+08:00", 4000, 10),
      Row(7965, "HUNT", "ANALYST", 7523, "2011-07-02T22:12:13.000+08:00", 4000, 20),
      Row(7435, "HUNT", "ANALYST", 7523, "2011-07-02T22:12:13.000+08:00", 5000, 20)
    ))

    val schema = StructType(List(
      // 提供 Row 中各字段的属性
      StructField("id", DataTypes.IntegerType, nullable = false),
      StructField("name", DataTypes.StringType, nullable = false),
      StructField("job", DataTypes.StringType, nullable = false),
      StructField("mgr", DataTypes.IntegerType, nullable = false),
      StructField("hiredate", DataTypes.StringType, nullable = false),
      StructField("salary", DataTypes.IntegerType, nullable = false),
      StructField("deptno", DataTypes.IntegerType, nullable = false)
    ))

    // 构建 DataFrame 对象
    val frame = sparkSession.createDataFrame(rows, schema)
    frame.printSchema()
    frame.show()

    sparkSession.stop()
  }
}
