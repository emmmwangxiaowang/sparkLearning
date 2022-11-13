import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/13
 */
object OtherTransformaionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("OtherTransformaionDemo")
      .setMaster("local")
    val sc = new SparkContext(conf)

    /**
     *  glom 将每一个分区中数据新城一个数组,新城新的 RDD 类型是 RDD[Array[T]
     */
    sc.makeRDD(Array.range(1,100),4)
      .glom()
      .foreach(a=> println(a.toList))

    /**
     * zip 将两个 RDD 组合成 key/value 类型的 RDD , 这里默认两个 RDD 的 partition 数量以及元素数量都相同, 否则会抛出异常
     */
    val intRDD: RDD[Int] = sc.makeRDD(Array.range(1, 6).toList,2)
    val strRDD: RDD[String] = sc.makeRDD(Array("a", "b", "c", "d", "e"),2)

    // 以 int 为 key
    intRDD
      .zip(strRDD)
      .collect()
      .foreach(println)

    // 以 str 为 key
    strRDD
      .zip(intRDD)
      .collect()
      .foreach(println)

    /**
     *  partitionBy 可以对 RD 进行重新分区操作, 但是参数是 Partitioner 类型
     *  不能给具体树脂作为分区一句, 如果原有 RDD 分区与 partitionBy 提供分区是一致
     *  它就不会进行分区操作, 否则就会产生 shuffle 操作
     */
    val pairIRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (3, "ccc"),(4,"dddd")), 4)
    pairIRDD.partitionBy(new HashPartitioner(2))
      .collect()
      .foreach(println)

    /**
     * foldByKey 提供的发分区内的计算规则和分区间计算规则是一样的
     * foldByKey 其实就是 aggregateByKey 简化版本
     * foldByKey 也存在默认值 zeroVale , 也会参与到分区内计算
     */
    val pairRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 12),("d",42), ("b", 32), ("c", 32),("d",21), ("c", 12), ("a", 12), ("b", 24)))
    pairRDD.foldByKey(0)(_+_)
      .collect()
      .foreach(println)

    /**
     * combineByKey  作用于 k-v RDD  对相同 K , 把 V 合并,形成一个新的 K-V
     * 参数描述：
     * （1）createCombiner : combineByKey() 会遍历分区中的所有元素,因此每个元素的键要么还没有遇到过,要么就和之前的某个元素的键相同。如果这是一个新的元素,combineByKey()会使用一个叫作createCombiner()的函数来创建那个键对应的累加器的初始值。
     * （2）mergeValue:如果这是一个在处理当前分区之前已经遇到的键,它会使用mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并。
     * （3）mergeCombiners:由于每个分区都是独立处理的, 因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器, 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
     * @详情:
     * https://blog.csdn.net/BigData_Hobert/article/details/108762358#combineByKeyC_641
     */
    pairRDD.combineByKey(
      ((x: Int) =>(x,1)),
      (acc:(Int,Int),v: Int)=>{
        println(acc._1+":"+acc._2)
        (acc._1+v,acc._2+1)
      },
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
      .map({
            // 最后的结果 是 (String,(Int,Int)) 类型  String 为 key , (Int,Int) 中 第一个 Int 为相同 key 的 value之和, 第二个 Int 为相同 key 的个数
        case (key,value) =>(key,value._1/value._2)
      })
      .collect()
      .foreach(println)

    /**
     *  groupBy
     *  将元素通过函数生成相应的 Key,数据就转化为 Key-Value 格式,之后将 Key 相同的元素分为一组。
     */
    intRDD
      .groupBy(_%2)
      .collect()
      .foreach(println)

    /**
     * filterByRange 对 RDD 中 二元组中的 key 进行指定范围内的 内容过滤操作
     * 得到返回结果时 包括过滤返回的 起始位置和结果位置
     */
    pairRDD
      .filterByRange("b","d")
      .collect()
      .foreach(println)

    /**
     * flatMapValues 对 RDD 中 二元组中的 value 值进行扁平化处理
     */
    sc.makeRDD(List(("a","2 3 4"),("b","1 2 3")))
      .flatMapValues(_.split(" "))
      .collect()
      .foreach(println)

    /**
     * keyBy 自定义 key
     */
    strRDD
      .keyBy(_.length)
      .collect()
      .foreach(println)

    /**
     * keys 获取 二元组中所有的 key
     */
    pairRDD
      .keys
      .collect()
      .foreach(println)

    /**
     * values 获取 二元组中所有的 value
     */
    pairRDD
      .values
      .collect()
      .foreach(println)

    /**
     *  mapValues 对 RDD 中的二元组 value 值进行 map 操作 将 value 换成 Boolean 类型 类似 predicate
     */
    pairRDD
      .mapValues(_>20) // 将结果 替换至 二元组中对应的 value
      .collect()
      .foreach(println)

    /**
     * countByValue 将 RDD 中每一个元素看作是一个 value 进行统计
     * 相同 value 值会进行累加统计
     */
    strRDD
      .countByValue()
      .foreach(println)

    pairRDD
      .countByValue()
      .foreach(println)

    /**
     * foreachPartition 对 RDD 中分区内的数据提供数据处理操作
     * 常用于分区的数据存储操作
     */
    intRDD
      // 每个分区 都是一个迭代器,
      .foreachPartition(x=>println(x.reduce(_+_)))

    /**
     *  collectAsMap 将 RDD 中的二元组 转换为 Map
     */
    pairRDD
      .collectAsMap()
      .foreach(println)

    /**
     * top 可以返回 num 个数据, 会对数据进行排序操作 默认降序
     * 与 takeOrdered 相反
     */
    intRDD
      .top(3)
      .foreach(println)
  }
}
