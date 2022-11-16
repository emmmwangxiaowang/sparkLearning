package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/15
 */
object SortDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val girlInfo: RDD[(String, Int, Int)] = sc.makeRDD(List(("a", 80, 35), ("b", 90, 53), ("c", 80, 73), ("d", 96, 47)))

    /**
     * Spark 提供了 两种排序算子
     *  sortByKey   和   sortBy
     */
    // 简单排序  根据第二个参数 降序
    girlInfo.sortBy(_._2,false)
      .collect()
      .foreach(println)

    /*
    如果第二个参数一致, 按照第三个参数降序排序,
    这时候就不能简单使用 sortBy, 需要自定义排序
     */
    // 使用 import 导入隐式转换操作
    import MyOrdering.girlOrdering
    girlInfo.sortBy(g => Girl(g._2, g._3))
      .collect()
      .foreach(println)

    girlInfo.sortBy(g=>Girls(g._2,g._3))
      .collect()
      .foreach(println)
  }
}

case class Girls(faceValue:Int,age:Int)  extends Ordered[Girls]{
  override def compare(that: Girls): Int = {
    if(this.faceValue!=that.faceValue){
      this.faceValue-that.faceValue
    }else{
      that.age-this.age
    }
  }
}