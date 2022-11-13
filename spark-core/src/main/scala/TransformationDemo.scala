import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/9
 */
object TransformationDemo {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 和 SparkContext 对象
    val conf: SparkConf = new SparkConf()
      .setAppName("TransformationsDemo").setMaster("local")

    val sc = new SparkContext(conf)

    // 学过 java8 Stream 流 感觉都差不多, 中间操作和终结操作,中间操作是流式计算的重点
    sc.makeRDD(Array(231, 231, 231, 123, 213, 123))
      .map(_*2)
      .filter(_>300)
      .foreach(println)

    sc.makeRDD(Array("a c v d d","e f g s x d"))
      .flatMap(_.split(" "))
      .collect()
      .foreach(println)

    // mapPartitions 将待处理的数据 "以分区位单位" 发送到计算节点进行处理
    sc.makeRDD(List(1,2,3,4,5,6,7),3)
      .mapPartitions(_.map(_*10))
      .collect()
      .foreach(println)

    /*
     * mapPartitionsWithIndex 将待处理的数据以分区位单位发送到计算节点进行处理,
     *  在处理时可以获取当前分区的索引
     *  第一个参数 f:(Int,Iterator[T])=> Iterator[U]
     *  元组中第一个参数就是分区序号, 第二个参数就是分区数据封装到迭代器对象
     *  第二个参数: preservesPartitioning: Boolean = false
    */
    sc.makeRDD(List(1,2,3,4,5,6,7),3)
      .mapPartitionsWithIndex((index:Int,iter:Iterator[Int])=>{
        iter.map(x=>"[partID:"+index+" value:"+x+" ]")
      })
      .collect()
      .foreach(println)

    /**
     *  sample 根据指定规则从数据集中抽取数据
     *    参数:
     *      withReplacement: Boolean, 表示抽取出数据之后是否返回原有的样例中 True 返回
     *      fraction: double 表示抽样的比例 当第一个参数为 false 时(抽样不放回) 范围为[0,1]
     *      seed: Long = Utils.random.nextLong  种子, 获取随机数据的方式, 默认不传值
     *
     */
    println(sc.makeRDD(seq = 1 to 1000)
      .sample(withReplacement = false, fraction = 0.7)
      .collect()
      .count(_ > 0))

    /**
     *  抽取数据不放回
     *    1. 0 1 分布
     *      根据种子和随机算法算出一个参数和第二个参数设置几率进行比较
     *      小于第二个参数要, 大于不要
     *      这个算法提供第二参数  范围[0,1] 按百分比获取样本中的数据
     *
     *  抽取数据不放回
     *    1. 泊松分布
     *      第一个参数 抽样是否放回 若为 true  则 抽样可能重复
     *      第二个参数: 重复数据的次数  范围, 大于1 表示 期望抽样的重复次数
     */

    // 这个结果不理解
    println(sc.makeRDD(seq = 1 to 1000)
      .sample(withReplacement = true, fraction = 3)
      .collect()
      .count(_ > 0))

    println(sc.makeRDD(seq = 1 to 1000)
      .sample(withReplacement = true, fraction = 7)
      .groupBy(a => a)
      .map(word => (word._1, word._2.size))
      .map(word => word._2)
      .sum()
    )

    sc.makeRDD(seq = 1 to 100)
      .sample(withReplacement = true, fraction = 7)
      .groupBy(a => a)
      .map(word => (word._1, word._2.size))
      .map(word => word._2)
      .groupBy(a=>a)
      .map(word=>(word._1,word._2.size))
      .sortBy(word=>word._1)
      .foreach(println)

    /**
     *  交并差
     */
    sc.makeRDD(List(1,2,3,4))
      .intersection(sc.makeRDD(List(2,3,54,6)))
      .collect()
      .foreach(println)

    sc.makeRDD(List(1,2,3,4))
      .union(sc.makeRDD(List(2,3,54,6)))
      .collect()
      .foreach(println)

    sc.makeRDD(List(1,2,3,4))
      .subtract(sc.makeRDD(List(2,3,54,6)))
      .collect()
      .foreach(println)

    // 根据键来 取差集
    sc.makeRDD(Array(("a","lily"),("b","lucy"),("c","rose"),("c",3),("d","jack")))
      .subtractByKey(sc.makeRDD(Array(("a",1), ("b",2), ("c",3))))
      .collect()
      .foreach(println)

    sc.makeRDD(Array(("a","lily"),("b","lucy"),("c","rose"),("c",3),("d","jack")))
      .groupByKey()
      .foreach(println)

    // 用 range 填充数组 flatMap 将 数组中的每个元素转换为 1 到 该元素
    sc.makeRDD(Array.range(1 , 10 ,2))
      .flatMap(ele=>1 to ele)
      // 当对 ele 没有操作时 语法糖简写 为 方法体
//     .flatMap(1 to _)
      .collect()
      .foreach(println)

    // glom 根据分区数来分组 有几个分区 分几组
    sc.makeRDD(Array.range(1,10,2),3)
      .glom()
      .collect()
      .foreach(println)

    // reduce 减少  reduceByKey  根据 key 分组减少
    sc.makeRDD(Array(("tom",1),("jack",2),("steam",3),("tom",3),("jack",2),("tom",3)))
      // 将相同 key 的元组 进行累加
//      .reduceByKey((a,b)=>a+b)
      // 参数 后续没有改变  所以 用 _代替
      .reduceByKey(_+_)
      .collect()
      .foreach(println)

    /**
     *  aggregateByKey 将数据根据 不同的规则 进行分区内计算  和 分区间计算
     *  在 kv 对的 RDD 中, 按照 key 将 value 进行分区内操作, 然后进行分区间操作
     *  方法的第一个参数是一个默认值, 这个值会参与到分区的计算中
     *  方法的第二个参数又两个参数
     *  seqOp:(U,V) => U 提供分区内的数据计算逻辑
     *  combOp: (U,V) => U 提供分区间的数据计算逻辑
     *
     *  这个算子主要时使用 zeroValue 这个参数参与到分区内计算, 分区 seqOp 内计算完毕之后再进行分区 comOp 之间的计算操作
     */

    sc.parallelize(List(("cat",2),("cat",4),("dog",5),("monkey",6),("cat",5),("dog",3),("cat",4)),2)
      .aggregateByKey(zeroValue = 0)((a,b)=>a+b,(a,b)=>a+b)
      .mapPartitionsWithIndex((index:Int,iter:Iterator[(String,Int)])=>{
        iter.map(x=>"[partID:"+index+" value:"+x+" ]")
      })
      .collect()
      .foreach(println)

    sc.parallelize(List(("cat",2),("cat",4),("dog",5),("monkey",6),("dog",3),("cat",5),("cat",4)),3)
      .aggregateByKey(zeroValue = 0)((a,b)=> if(a>b) a else b ,_+_)
      .mapPartitionsWithIndex((index:Int,iter:Iterator[(String,Int)])=>{
        iter.map(x=>"[partID:"+index+" value:"+x+"]")
      })
      .collect()
      .foreach(println)

    // sortByKey 第一个参数 确定升序还是降序 true---升序
    sc.makeRDD(Array((3,"a"),(4,"b"),(6,"c"),(0,"d")),2)
      .sortByKey(true)
      .foreach(println)

    // range 是 until
    // sortby 可以先对元素进行 自定义排序
    sc.makeRDD(Array.range(1 ,20))
      .sortBy(x=>x%3,false)
      .foreach(println)

    /**
     *  join 再类型为(K,V) 和(K,W) 的RDD 上调用, 返回一个相同key 对应的所有元素
     *  连接在一起的( K,(V,W)) 的RDD
     *  相同 key 的 value 会合并在一起, 没有相同key 的将被舍弃
     */
    val rdd1_1: RDD[(String, Int)] = sc.makeRDD(List(("tom", 1), ("jack", 3), ("merry", 2)))
    val rdd1_2: RDD[(String, Int)] = sc.makeRDD(List(("tom", 1), ("jack", 3), ("wang", 2)))
    // Map<String,List<Integer>> 将 两个 RDD 中都有的key 放到这个集合中, 没有的舍弃
    val rdd1_3: RDD[(String, (Int, Int))] = rdd1_1.join(rdd1_2)
    rdd1_3
      .collect()
      .foreach(println)

    /**
     *  leftOuterJoin 类似于 SQL 中做链接, 返回以调用方法的 RDD 为主, 关系不上记录为空
     *  rightOuterJoin 类似与 Sql 中右链接, 返回以方法参数的 RDD 为主, 关系不上记录为空
     */
    val value: RDD[(String, (Int, Option[Int]))] = rdd1_1.leftOuterJoin(rdd1_2)
    println(rdd1_1.leftOuterJoin(rdd1_2).collect().toList)
    println(rdd1_1.rightOuterJoin(rdd1_2).collect().toList)


    /**
     *  cogroup 在 当前类型为 (K,V) 和 (K,W) 的 RDD 上调用, 返回一个 (K,(Iterable<V>,Iterable<W>) 类型的 RDD
     *  提供操作的分组必须是一个对偶元组
     */
    val rdd2_1: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1_1.cogroup(rdd1_2)
    rdd2_1.collect()
      .foreach(println)

    /**
     *  coalesce 可以根据数据链 "缩减分区" 用于大数据集过滤之后,条小数据集的执行效率
     */

    val rdd3_1: RDD[Int] = sc.makeRDD(List.range(1, 100), 3)
    println(rdd3_1.partitions.length)
    val rdd3_2: RDD[Int] = rdd3_1.coalesce(1)
    println(rdd3_2.partitions.length)

    /**
     *  repartition 内部执行 其实就是  coalesce  只不过将 coalesce 方法中 shuffle 参数赋值为 true,
     *  无论是将分区数多的 RDD 转换为 分区数少 RDD 还是将分区数少的 RDD 转换为当前分区多的 RDD
     *   repartition 都可以完成, 因为无论怎么样操作 都要经过 shuffle
     *   
     *  repartition 与 coalesce
     *    本身需要进行分区合并的时候使用coalesce是最合适的,原因就在于coalesce默认shuffle是false,
     *    也就是说,如果100个分区 想要变成50个分区,这个时候coalesce并不会进行shuffle操作。
     *      进行coalesce的场景,数据分区很多,但是每个分区数较少,这个时候其实并不太影响执行效率,
     *      但是对于一些需要外部链接,或者写入文件的场景来说,这是很浪费资源的。

     *      需要使用shuffle进行分区数减少的场景,原始的rdd分区数据分散不是很均匀,比如 当前RDD是100个分区,
     *      想要合并成50个分区,但是100个分区总数据分布从10K-200M分配不均,这个时候为了方便下游数据处理,我们可以将数据进行shuffle形式的合并。
     */

    println(rdd3_1.repartition(2).partitions.length )

    /**
     *  repartitionAndSortWithinPartitions
     *    对集合进行分区且排序,这个算子只能对二元组使用
     *    会根据 key 进行排序,因为这个排序是与 shuffle 同时 进行的, 比先分区再排序要快
     *    所以 需要 分区后进行排序时,可以使用这个算子进行代替
     */
    sc.makeRDD(List(("e",231),("f",431),("e",342),("f",532),("e",532),("a",321)),1)
      // 自定义 partitioner, 根据 key 定义了三个分区, 保证分区内有序
      .repartitionAndSortWithinPartitions(new KeyBasePartitioner(3))
      .collect()
      .foreach(println)

    /**
     * 自定义分区器
     *
     * @param partitions
     */
    class KeyBasePartitioner(partitions: Int) extends Partitioner {
      //分区数
      override def numPartitions: Int = partitions
      //该方法决定了你的数据被分到那个分区里面
      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[String]
        Math.abs(k.hashCode() % numPartitions)
      }
    }



  }

}
