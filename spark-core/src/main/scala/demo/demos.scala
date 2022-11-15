package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/14
 */
object demos {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("demos").setMaster("local")
    val sc = new SparkContext(conf)

    /**
     *  求 每本图书的日均销量  三种实现方式
     */
    val bookRDD: RDD[(String, Int)] = sc.makeRDD(List(("spark", 12), ("hadoop", 43), ("spark", 53), ("hadoop", 25)))
    bookRDD
      .combineByKey(
        ((x:Int)=>(x,1)),
        ((acc:(Int,Int),v:Int)=>{
          ((acc._1+v),acc._2+1)
        }),
          (acc1:(Int,Int),acc2:(Int,Int))=>{
            ((acc1._1+acc2._1),(acc1._2+acc2._2))
          })
      .map({
        case (key,value)=>(key,value._1/value._2)
      })
      .collect()
      .foreach(println)

    bookRDD
      .groupByKey()
      .map(x=>(x._1,x._2.sum/x._2.size))
      .collect()
      .foreach(println)

    bookRDD
      // 先对 value 进行操作, 变成(value,1)
      .mapValues(x=>(x,1))
      // 再对 value 进行遍历,将各元素分别相加, 求出每本书卖的总数 和 天数
      .reduceByKey((x,y)=>((x._1+y._1,(x._2+y._2))))
      // 对 value 进行处理 , 用总数 / 天数 就为平均值
      .mapValues(x=>x._1/x._2)
      .collect()
      .foreach(println)

    /**
     *  文本数据读取 过滤掉空数据 求出最大值和最小值
     */
    sc.textFile("spark-core\\data\\a.txt")
      .filter(x=>x.trim.nonEmpty)
      // 对每个元素生成一个相同的 key 变成 多个 拥有相同 key 的 二元组
      .map(x=>("key",x.trim.toInt))
      // 将各个二元组进行合并 因为 key 相同 所以就是一个 (key,iter)
      .groupByKey()
      .map(x=>{
        var min = Integer.MAX_VALUE
        var max = Integer.MIN_VALUE

        for( num <- x._2){
          if(num>max)
            max=num
          if(num<min)
            min=num
        }
        (max,min)
      })
      .foreach(println)

    val studentRDD: RDD[(String, Array[String])] = sc.textFile("spark-core\\data\\student.txt")
      .map(x => {
        val strings: Array[String] = x.split(" ")
        (strings(1), strings)
      })
    // 一共有多少人参加三门考试
    studentRDD
      .keys
      .distinct()
      .count()

    // 一共由多少小于 20 岁的人参加考试
    studentRDD
      .filter(x=>
          x._2(2).toInt<20
      )
      .keys
      .distinct()
      .count()

    // 一共由多少男生参加考试
    studentRDD
      .filter(x=>
        x._2(3).equals("男")
      )
      .keys
      .distinct()
      .count()

    // 13 班有多少人参加考试
    println(studentRDD
      .filter(x =>
        x._2(0).toInt.equals(13)
      )
      .keys
      .distinct()
      .count())

    // 语文科目平均成绩
    studentRDD
      .filter(x=>{
        x._2(4).equals("chinese")
      })
      // 要球的是平均分, 所以不用去重 , 直接对所有所有数据加一个 key -- chinese
      // 之后在进行 聚合 形成 ( chinese,iter)  之后对 iter 进行平均值计算
      .map(x=>{
        ("chinese",x._2(5).toInt)
      })
      .groupByKey()
      .map(x=>{
        (x._1,x._2.sum/x._2.size)
      })
      .collect()
      .foreach(println)

    // 12 班 平均成绩
    studentRDD
      .filter(x=>{
        x._2(0).equals("12")
      })
      .map(x=>{
        ("12班",x._2(5).toInt)
      })
      .groupByKey()
      .map(x=>{
        (x._1,x._2.sum[Int]/x._2.size)
      })
      .collect()
      .foreach(println)

    // 12 班 男生平均总成绩
    studentRDD
      .filter(x=>{
        x._2(3).equals("男") && x._2(0).equals("12")
      })
      .map(x=>{
        ("12 班 男",x._2(5).toInt)
      })
      .groupByKey()
      .map(x=>{
        (x._1,x._2.sum/x._2.size)
      })
      .collect()
      .foreach(println)

    // 全校英语成绩最高分
    studentRDD
      .map(x=>{
        ("英语",x._2(5).toInt)
      })
      .groupByKey()
      .map(x=>{
        (x._1,x._2.max)
      })
      .collect()
      .foreach(println)

    // 13 班数学最高成绩
    studentRDD
      .filter(x=>{
        x._2(0).equals("13") && x._2(4).equals("math")
      })
      .map(x=>{
        ("13 班 math",x._2(5).toInt)
      })
      .groupByKey()
      .map(x=>{
        (x._1,x._2.max)
      })
      .collect()
      .foreach(println)
  }

}
