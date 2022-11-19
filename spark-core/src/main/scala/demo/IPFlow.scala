package demo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.PreparedStatement

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/19
 */
object IPFlow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IPFlow").setMaster("local")
    val sc = new SparkContext(conf)

    val ipInfo: RDD[(Long, Long, String)] = sc.textFile("spark-core/data/IP/ip.dat")
      .map((line: String) => {
        val fields: Array[String] = line.split("\\|")
        // 获取数据中起始 IP 地址转换值, 结束 IP 地址转换值和省份
        val startIP = fields(2).toLong
        val endIP = fields(3).toLong
        val province = fields(6)
        (startIP, endIP, province)
      })


    /*
      在后续处理日志信息文件是需要使用 RDD 中所引用到具体数据而不是这个RDD
      所以需要将拆分后的数据的 RDD 具体化成数据信息 --> 将 RDD 引用数据转换为具体数据
      而转换为具体数据就会变成 "本地变量", 所以需要将本地变量 广播化
     */
    // 提供广播变量
    val broadcastIPInfo: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipInfo.collect)

    broadcastIPInfo.value.foreach(println)

    val provinceData: RDD[(String, Int)] = sc.textFile("spark-core/data/IP/http.log")
      .map((line: String) => {
        val fields: Array[String] = line.split("\\|")
        // 获取 IP 地址转换 后的 数据值
        val ip: String = fields(1)
        val ip2Long: Long = ipToLong(ip)
        // 需要从广播变量中将存的数据获取回来进行查找操作
        val ipInfos: Array[(Long, Long, String)] = broadcastIPInfo.value
        // 提供一个查找方法进行数据查找操作(因为读取数据文件时,数据本身就是有序的,所以直接可以使用二分查找)
        val index: Long = binarySearch(ipInfos, ip2Long)
        if(!index.intValue().equals(-1)){
          val province: String = ipInfos(index.toInt)._3
          (province, 1)
        }else{
          ("none",1)
        }

      })

    provinceData.collect().foreach(println)
    // 统计访问数据
    val sum: RDD[(String, Int)] = provinceData.reduceByKey((_: Int) + (_: Int))

    sum.foreachPartition(data2MySQL)

    sc.stop()
  }

  def ipToLong(ip:String)={
    val fragments: Array[String] = ip.split("\\.")
    var ipNum=0L
    for(i <- 0 until fragments.length){
      // ipv4 用 32 位 2进制表示, 这里将 10进制的ip a.b.c.d 分成 4 份,初始 ipNum 为 0, 与 a,b,c,d 进行 或运算,
      // 每一次算完之后左移8位,就是右端补0,因为 | 运算是 右端对齐,所以就变成了 b 和0 进行 | 运算,以此类推
      ipNum = fragments(i).toLong | ipNum << 8L
      println(fragments(i))
    }
    println(ipNum)
    ipNum
  }

  def binarySearch(arr:Array[(Long, Long, String)],ip:Long): Long ={
    // 提供开始值 和结束值
    var start = 0
    var end: Int = arr.length-1
    while (start <= end){
      // 求中间值
       val mid: Int = (start + end) / 2
      if((ip >= arr(mid)._1)&&(ip <= arr(mid)._2)){
        // scala 不推荐使用 return
        return mid
      }else if(ip<arr(mid)._1){
        end = mid -1
      }else{
        start = mid + 1
      }
    }
    // 找不到 返回 -1
     -1
  }

  // 向数据库中写入数据
  val data2MySQL =(iter:Iterator[(String,Int)])=>{
    // 创建连接对象
    val conn = java.sql.DriverManager.getConnection(
      "jdbc:mysql://124.220.74.230:3306/spark",
      "root",
    "123456"
    )
    // 提供预编译 sql
    val sql:String= "insert into location_infos(location,counts) values(?,?)"
    var ps:PreparedStatement = null
    for (elem <- iter) {
        ps=conn.prepareStatement(sql)
      ps.setString(1,elem._1)
      ps.setInt(2,elem._2)
      ps.executeUpdate()
    }

    ps.close()
    conn.close()
  }

}
