package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/14
 */
object BaseStationDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseStationDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val files: RDD[String] = sc.textFile("spark-core\\data\\log\\*")

    // 切分文件中用户的 log 信息
    val user_info: RDD[(String, (String, Long))] = files.map(x => {
      val fields: Array[String] = x.split(",")
      val phone: String = fields(0)
      val time: Long = fields(1).toLong
      val lac: String = fields(2)
      val flag: String = fields(3)
      // flag ==0 表示 离开基站,停留时长 ==  离开时间 - 进入时间 ,
      val time_long: Long = if (flag.equals("0")) time else -time // 最后求停留时间只需将两个时间相加
      // 谁 在 哪个基站 停留了多长时间
      ((phone, lac), time_long)
    })

      // 求出 某个基站的停留时长
      .reduceByKey(_ + _)
      .map(x => {
        (x._1._2, (x._1._1, x._2))
      })

    user_info
      .collect()
      .foreach(println)

    val lac_info: RDD[(String, (String, String))] = sc.textFile("spark-core\\data\\lac_info.txt")
      .map(x => {
        val fields: Array[String] = x.split(",")
        val lac: String = fields(0)
        val jin: String = fields(1)
        val wei: String = fields(2)
        (lac, (jin, wei))
      })


    user_info
      // 用户信息与基站信息 合并
      .join(lac_info)
      // 调整数据 为 phone,time,(jin,wei)
      .map(x=>{
        (x._2._1._1,x._2._1._2,(x._2._2))
      })
      // 将 phone 设为 key
      .groupBy(_._1)
      // 对 value 对时间进行排序
      .mapValues(_.toList.sortBy(_._2).reverse)
      .map(x=>{
        val phone: String = x._1
        val list: Seq[(Long, (String, String))] = x._2
          .map(x=>{
            val time: Long = x._2
            val jin_wei: (String, String) = x._3
            (time,jin_wei)
          })
        (phone,list)
      })
      .mapValues(_.take(2))
      .collect()
      .foreach(println)


  }
}
