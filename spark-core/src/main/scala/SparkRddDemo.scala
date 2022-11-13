import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/9
 */
object SparkRddDemo {
  def main(args: Array[String]): Unit = {
    // 先创建 SparkConf 和 SparkContext 对象
    val conf: SparkConf = new SparkConf().setAppName("SparkRddDemo").setMaster("local")
    val sc = new SparkContext(conf)

    /**
     * // 从 集合(内存) 中创建 RDD -- 一般用于测试使用
     * RDD 本身并不存储数据, 是对数据集的抽象  是一个数据集的引用
     * RDD[Int] 表示 RDD 操作的数据类型是 Int 类型 -- RDD 抽象的数据集是 Int 类型
     */
    // makeRDD 第二个参数 指定分区数量
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

    // 从外部文件 创建 RDD
    val rdd3: RDD[String] = sc.textFile(path = "C:\\Users\\Administrator\\Desktop\\cm3.json")

    // 从其他的 RDD 创建
    val rdd4: RDD[String] = rdd3.flatMap((_: String).split(" "))
  }
}
