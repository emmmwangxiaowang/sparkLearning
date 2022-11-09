import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/10/28
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    /*
        配置spark,设置任务名称,不设置默认为 uuid 所产生的名字
        设置运行模式
        不提交到集群中运行即 编译 jar包 提交到集群 需要 setMaster  -- 本地模式
        "local" 本地运行 提供一个线程进行任务处理
        "local[数值]" 开启相应数值的线程数 模拟 spark 集群运行 任务
        "local[*]" 根据当前空闲的线程数 模拟 spark集群运行任务
    */
    val conf = new SparkConf().setAppName("SparkWordCount")

    // 创建 SparkContext 对象
    var sc = new SparkContext(conf)

    // 1.读取文件 参数是 String 类型
    val lines: RDD[String] = sc.textFile(args(0))

    // 2. 切分当前读取到的数据
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. 将每一个单词生成一个对偶元组
    val tuples: RDD[(String, Int)] = words.map((_, 1))

    // 4. 利用 spark 提供的 算子 reduceByKey -- 相同 key 为一组进行计算 value 值
    val sums: RDD[(String, Int)] = tuples.reduceByKey(_ + _)

    /*
        5. 对结果进行排序 spark 提供的算子 sortBy
           和 scala 中的 sortBy 不一样, spark 提供了一个参数 进行升降序, 默认升序
     */

    val sorted: RDD[(String, Int)] = sums.sortBy(_._2, false)

    //6. 将数据提交到 HDFS 中进行存储,没有返回值
    sorted.saveAsTextFile(args(1))

    // 如果是本地模式,一般是会打印 println(sorted.collect.buffer) / sorted.foreach(pringln)

    //7. 结束 SparkContext 回收资源
    sc.stop()

  }
}
