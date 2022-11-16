package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/16
 */
object AccmulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AccmulatorDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6),2)

//    var sum=0
//
//    rdd.foreach(num=>{
//      sum+=num
//      println("excutor中:"+sum)
//    })
//
//    println("driver 中:"+sum)

    // Accumulator 会将各个 executor 中的 数据进行聚合操作 返回 driver
    var sums=sc.longAccumulator("acc")
    rdd.foreach(num=>{
      sums.add(num)
      println("executor:"+sums.value)
    })

    println("driver:"+sums.value)
  }


}
