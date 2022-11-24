package demos

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/21
 */
object UserActAnalize {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("UserActAnalize")
      .master("local[*]")
      .getOrCreate()





    import sparkSession.implicits._
    val cityFrame: DataFrame = sparkSession.read.text("spark-sql/data/userAct/city_info.txt").rdd
      .map(Row => {
        Row.get(0).toString
      })
      .map(line => {
        val fields: Array[String] = line.split("\\t")
        (fields(0), fields(1), fields(2))
      })
      .toDF("city_id", "city_name", "area")
    //               city_id  city_name  area
    // city_info.txt  1	北京	华北
    val city: Unit = cityFrame.createOrReplaceTempView("city")

    val productFrame: DataFrame = sparkSession.read.text("spark-sql/data/userAct/product_info.txt").rdd
      .map(Row => {
        Row.get(0).toString
      })
      .map(line => {
        val fields: Array[String] = line.split("\\t")
        (fields(0).toInt, fields(1), fields(2))
      })
      .toDF("prod_id", "prod_name", "prod_info")
    //                 prod_id  prod_name prod_info
    // product_info.txt  1	商品_1	自营
    val product: Unit = productFrame.createOrReplaceTempView("product")

    // date    user_id   session_id  page_id  action_time   search_keyword  click_category_id  click_product_id  order_category_ids  order_product_ids  pay_category_ids  pay_product_ids  city_id
    // user_visit_action.txt  2019-07-17	39	e17469bf-    	19	      2019-07-17  	\N	              -1	                -1	                1,19,17,3,14	    99,46	               \N	             \N	            20

    val userFrame: DataFrame = sparkSession.read.text("spark-sql/data/userAct/user_visit_action.txt").rdd
      .map(_.toString())
      .map(line => {
        val fields: Array[String] = line.split("\\t")
        (fields(7).toInt, fields(12).split("]")(0).toInt)
      })
      .toDF("prod_id", "city_id")
    val user: Unit = userFrame.createOrReplaceTempView("user")


//    sparkSession.udf.register("remark",new myUDAF)
    sparkSession.udf.register("remark",functions.udaf(new CityRemarkUDAF))
    userFrame.select("prod_id", "city_id")
      .join(cityFrame, "city_id")
      .join(productFrame, "prod_id")
    //    sparkSession.sql("select u.prod_id,u.city_id,c.city_name,c.area,p.prod_name from user u , product p,city c where u.prod_id=p.prod_id and u.city_id= c.city_id ").show()
    .createOrReplaceTempView("unionTable")
    sparkSession
      .sql(
        """
          | select
          |   area,
          |   prod_name,
          |   count(*) count,
          |   remark(city_name) remark
          |   from unionTable group by area, prod_name
          |""".stripMargin)
      .createOrReplaceTempView("tempTable")

    // 使用窗口函数 实现 分组 排序
    sparkSession
      .sql("select prod_name,area,count,remark ,row_number() over(partition  by area order by count desc) rk from tempTable")
      .createOrReplaceTempView("resTempTable")
    // 取 topn
    sparkSession
      .sql("select area,prod_name,count ,remark from resTempTable where rk<=3")
      .show()

//
//      .groupby("area","prod_name")
//    // 对 点击量进行排序
//      .orderBy(desc("count"))
//    midTempTable.createOrReplaceTempView("tempTable")
    // 使用 rdd 实现 分组 topn
//    val midRes: DataFrame = midTempTable
//      .rdd
//      .map(line => {
//        (line(0), line(1), line(2))
//      })
//      // 根据区域 进行分组
//      .groupBy(_._2)
//      .map(line => {
//        // 分组后 value 中还有 area 字段  把 area 字段踢出去
//        val tuples: Iterable[(Any, Any)] = line._2.map(value => {
//          (value._1, value._3)
//        })
//        // 获取 区域前三数据
//        val res: List[(Any, Any)] = tuples.take(3).toList
//        // for yield 中 yield 相当于 是 做了后面的事(每次循环都会有一个返回值) 之后 continue
//        // for yield 中 yield 充当了 return 的作用, 但是 return后并不会退出循环 将结果返回后 继续循环,知道到达循环结束条件
//        for (i <- 0 until 3) yield (line._1.toString, res(i)._1.toString,Integer.parseInt(res(i)._2.toString))
//      })
//      .flatMap(line => {
//        for (i <- 0 until 3) yield line(i)
//      })
//
//      .toDF("area", "prod_name", "click")
//    midRes
//      .show()






  }

//  class myUDAF extends Aggregator[]
}
