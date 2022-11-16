package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class CustomPartitioner(numPartition:Int) extends Partitioner {
  // 分区数
  override def numPartitions: Int = numPartition

  // 根据 key 返回分区索引
  override def getPartition(key: Any): Int = {
    val num: Int = key.hashCode() % numPartition
    if(num<0){
      println("num<0")
      num+numPartition
    }else{
      num
    }
  }
}

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/15
 */
object CustomPartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomPartitioner").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, Long)] = sc.makeRDD(0 to 10, 1)
      // 将 RDD 中的元素和这个元素所在 RDD 的 ID(索引) 进行组合形成键值对
      .zipWithIndex()

    val func =(index:Int,iter:Iterator[(Int,Long)])=>{
      iter.map(x=>"[partID:"+index+",value:"+x+"]")
    }
    val collect: Array[String] = rdd.mapPartitionsWithIndex(func).collect()

    collect.foreach(println)

    // 使用自定义分区器进行分区操作
    val rdd2: RDD[(Int, Long)] = rdd.partitionBy(new CustomPartitioner(2))
    val collect2: Array[String] = rdd2.mapPartitionsWithIndex(func).collect()
    collect2.foreach(println)
  }
}
