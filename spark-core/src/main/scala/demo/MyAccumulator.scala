package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
import scala.collection.immutable.HashMap

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/16
 */
class MyAccumulator extends AccumulatorV2[String,HashMap[String,Int]]{

  // 提供一个 hashMap 对象 进行结果操作
  private var _hashAcc = new HashMap[String, Int]

  // 判断当前累加器是否初始化
  override def isZero: Boolean = {
    _hashAcc.isEmpty
  }

  // 拷贝一个新的累加器
  override def copy(): AccumulatorV2[String, HashMap[String, Int]] = {
    val newAcc = new MyAccumulator
    _hashAcc.synchronized{
      newAcc._hashAcc ++= (_hashAcc)
    }
    newAcc
  }

  // 重置累加器
  override def reset(): Unit = {
    _hashAcc.clear()
  }

  // 向累加器中增加值
  override def add(k: String): Unit = {
    _hashAcc.get(k) match {
      case None => _hashAcc+=((k,1))
      case Some(v)=> _hashAcc+=((k,v+1))

    }
  }

  // 合并 所有分区内的累加器之和 --计算分区间的数据
  // 合并当前累加器和其他累加器,两两合并,此方法由 Driver 端调用,合并由 executor 返回的多个累加器
  override def merge(other: AccumulatorV2[String, HashMap[String, Int]]): Unit = {
    other match {
      case o:AccumulatorV2[String, HashMap[String, Int]]=>{
        for((k,v)<-o.value){
          _hashAcc.get(k) match {
            case None =>_hashAcc+=((k,v))
            case Some(a) => _hashAcc+=((k,a+v))
          }
        }
      }
    }
  }

  // 返回累加器中的数据值
  override def value: HashMap[String, Int] = {
    _hashAcc
  }
}

object MyAccumulator{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyAccumulator").setMaster("local")
    val sc = new SparkContext(conf)

    // 创建自定义累加器对象
    val hashAcc = new MyAccumulator

    // 注册累加器
    sc.register(hashAcc,"www")

    // 提供rdd
    val rdd: RDD[String] = sc.makeRDD(Array("a", "d", "s", "a", "c", "c", "a"), 2)

    rdd.foreach(hashAcc.add)
    for ((k,v)<-hashAcc.value){
      println("["+k+":"+v+"]")
    }

    sc.stop()


  }
}
