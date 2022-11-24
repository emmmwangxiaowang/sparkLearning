package grammer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/19
 */
object demo01 {
  def main(args: Array[String]): Unit = {
    // 提供 SparkSession 对象
    val sparkSession = SparkSession.builder()
      .appName("demo01")
      .master("local[*]")
      // .enableHiveSupport() // 开启 hive 支持
      .getOrCreate()

    // SparkSQL 支持两套语法
    // 加载 json 文件
    val frame: DataFrame = sparkSession.read.json("spark-sql/data/emp.json")
    // 1.DSL  结合 SQL 中关键字作为函数(算子) 的名字传递参数进行编程方式  -- 接近于 RDD 编程

    frame.printSchema()
    frame.show() // select * from table
    frame.select("JOB","MGR").show() // select 可以指定 列名
    // 导入 SparkSession 中隐式转换操作, 增强 sql 功能
    import sparkSession.implicits._
    frame.select($"JOB",$"MGR").show
    // $ 符 取值, --> 涉及到 列运算的时候使用
    // 也可以使用 '字段名
    frame.select($"ENAME".as("wang"),($"MGR"-1).as("jiang"),('DEPTNO+10).as("chen")).show()
    // 支持 各种操作
    frame.select("ENAME","JOB","MGR").where("DEPTNO=30").groupBy("JOB").count().show()


    // 2.SQL 语法 -- 直接写 SQL 或 HQL 语言进行编程  -- 现在 SparkSQL 的主流
    // 使用 SQL 的前提是需要将数据转换成表
    /*

      提供三种表
      createOrReplaceTempView --> 创建或替换临时视图, 作用域 SparkSession 生命周期内有效
      createOrReplaceGlobalTempView --> 创建或替换全局临时视图, 作用域在 Spark Application 生命周期内有效
      createGlobalTempView  --> 创建全局临时视图, 作用域在 Spark Application 生命周期内有效
      参数都是 创建表的名字
      使用全局临时表的时候需要全路径访问: 例如 global_temp.table_name
     */
    frame.createGlobalTempView("employee")
    sparkSession.sql(
      """
        | select * from global_temp.employee
        """.stripMargin).show()

    frame.createOrReplaceTempView("employee_1")
    sparkSession.sql(
      """
        |select
        |DEPTNO,
        |count(*) as counts
        |from
        |employee_1
        |group by DEPTNO
        """.stripMargin).show()

    sparkSession.stop()
  }
}
