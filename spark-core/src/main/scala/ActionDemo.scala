import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/13
 */
object ActionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ActionDemo")
      .setMaster("local")
    val sc = new SparkContext(conf)

    /**
     *  reduce  聚集 RDD 中所有的元素, 先聚合分区内数据, 然后聚合分区间的数据
     */
    println(sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3), ("b", 2), ("c", 2), ("a", 3)))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2)))

    /**
     *  collect 讲 RDD 转换为 Array 类型 存数据 -- Array 时不可变的数组, 可以使用 toxxx 转换为其他的数据结构
     *   驱动程序( Driver) 中, 以数组 Array 的形式返回数据集的所有元素
     *
     */
    val intRDD: RDD[Int] = sc.makeRDD(Array.range(1, 10, 2))

    intRDD
      .collect()
      .toList
      .foreach(println)

    /**
     *  count 返回当前 RDD 中的元素个数
     */
    println(intRDD
      .count())

    /**
     *  first 返回 RDD 中第一个元素
     */
    println(intRDD
      .first())

    /**
     *  take 返回由一个由 RDD 的前 n 个元素组成的数组
     */
    intRDD
      .take(2)
      .foreach(println)

    /**
     * takeSample 该函数会返回一个根据抽样设置好的 n 个数据的数组,适用于样本容量较小的抽样
     * withReplacement: Bollean, 抽样是否返回
     * true 是否可以重复 多次抽样  false 不可以重复多次抽样
     *  num : int 抽样个数
     */
    intRDD.takeSample(false,8) // num  大于 RDD 中的元素个数, 抽取的就是 RDD 中的所有元素
      .toList
      .foreach(println)

    intRDD.takeSample(false,2)
      .toList
      .foreach(println)

    intRDD.takeSample(true,8) // 抽样放回
      .toList
      .foreach(println)

    intRDD.takeSample(false,2) //
      .toList
      .foreach(println)

    /**
     *  takeOrdered 返回改 RDD 排序后的前 n 个元素所组成数组(默认升序)
     *  如果需要指定排序,则需要实现函数中隐式参数 Ordering 特指
     */
    intRDD.takeOrdered(3)
      .toList
      .foreach(println)

    /**
     * countByKey 统计 RDD 中对偶元组中相同 key 的个数
     * 返回值是 一个 Map 结合 key 是需要统计的 key , value 是 相同 key 的个数
     */
    val tupleRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 3), ("c", 3), ("b", 12), ("c", 21), ("a", 43), ("b", 23), ("a", 3)))
    println(tupleRDD.countByKey())

    /**
     * countByValue 统计 RDD 中相同元素 值 的个数 , 返回的类型为 Map[K,V],K:元素的值, V: 元素对应的个数
     */
    println(tupleRDD.countByValue())

    /**
     *  RDD 文件的读取于保存
     *  spark 的数据读取以及数据保存可以由两个维度来区分  文件格式 与 文件系统
     *  文件格式: text 文件,json 文件, csv 文件 , sequence 文件 以及 Object 文件
     *  文件系统分为: 本地系统, HDFS HBASE 以及 数据库
     *  https://www.cnblogs.com/LXL616/p/11147909.html
     */


  }

}
