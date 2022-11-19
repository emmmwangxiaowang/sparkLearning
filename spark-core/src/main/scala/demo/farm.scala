package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/18
 */
object farm {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("farm").setMaster("local")
    val sc = new SparkContext(conf)

    val textRDD: RDD[String] = sc.textFile("spark-core/data/farm/product.txt")

    // 1. 统计每个省份的农产品数量
    val productRDD: RDD[(String, Float, String, String, String, String)] = textRDD.map(x => {
      val values: Array[String] = x.split("\t")
      if (values.length == 6) {
        var name = values(0)
        var price =0.0f
        if(!values(1).isEmpty){
          price = values(1).toFloat
        }
        var date = values(2)
        var addr = values(3)
        var province = values(4)
        val city = values(5)
        (name, price, date, addr  , province, city)
      } else {
        ("", 0.0f, "", "", "", "")
      }
    }).persist()
    productRDD
      .map(x=>{
        (x._5,x._1)
      })

      .groupBy(x=>x._1)
      .distinct()
      .map(x=>{
        (x._1,x._2.size)
      })

      .collect()
      .foreach(println)

    productRDD.unpersist()

    // 2. 统计哪些农产品市场在售卖樱桃,标明 省份和城市
    productRDD
      .filter(x=>{
        x._1.equals("樱桃")
      })
      .map(x=>{
        (x._4,x._5,x._6)
      })
      .collect()
      .foreach(println)

    /*
     3. 统计山东生售卖蛤蜊的农产品市场占全省农产品市场的比例
    */

    val shandRDD: RDD[(String, Float, String, String, String, String)] = productRDD
      .filter(x => {
        x._5.equals("山东")
      })
    // 先统计出山东有多少农产品市场
    val shand: Long = shandRDD
      .groupBy(_._4)
      .count()

    // 再统计出有多少卖蛤蜊的
    val geli: Long = shandRDD
      .filter(_._1.equals("蛤蜊"))
      .count()
    println(geli*1.0/shand)

    /*
    统计农产品类型排名前三的省份共同拥有的农产品类型
     */
    val top3RDD: RDD[(String, String)] = productRDD
      .map(x => {
        // 省份名  蔬菜
       (x._5,x._1)
      })
      .distinct()
    // 1 先求出排名前三的省份
    // (北京,169)
    // (江苏,167)
    // (山东,134)
    top3RDD
      .map(x=>{
        (x._1,1)
      })
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(3)
      .foreach(println)

    // 2 如果top3 省份都有的蔬菜, 呢么做成 wordcount 最后 count 为 3
    top3RDD
      // 找到 top3
      .filter(x=>{
        x._1.equals("北京")||x._1.equals("江苏")||x._1.equals("山东")
      })
      // 做成 wordcount
      .map(x=>{
        (x._2,1)
      })
      // 求和
      .reduceByKey(_+_)
      // 保留 top3 都有的
      .filter(x=>{
        x._2.equals(3)
      })
      .collect()
      .foreach(println)

    // TreeSet 的排序是一个隐式参数, 所以自定义一个隐式函数,隐式参数会自己找对应的转换函数
    import SortFloat.sortFloat
    // 东北三省农产品最高价格降序排列，统计前十名的农产品有哪些
    productRDD
      .filter(x=>{
        x._5.equals("辽宁")||x._5.equals("黑龙江")||x._5.equals("吉林")
      })
      .map(x=>{
        (x._1,x._2)
      })
      .combineByKey(
        (first:Float)=> {
          val arr = new mutable.TreeSet[Float]()
          arr.add(first)
          arr
        },
        (arr:mutable.TreeSet[Float], v:Float)=>{
          arr.add(v)
          arr
        },
        // 分区间合并
        (arr1:mutable.TreeSet[Float], arr2:mutable.TreeSet[Float])=>{
          var data:Float=0.0f
          if(arr1(0)>arr2(0)){
            data= arr1.head
          }else{
            data= arr2.head
          }
          // merge 要保证 出入类型一致 所以 要返回一个 TreeSet
          val tuple: (mutable.TreeSet[Float], mutable.TreeSet[Float]) = arr1.splitAt(1)
          tuple._1.add(data)
          tuple._1
        }
      )
      // 总排序
      .sortBy(x=>{
        x._2.firstKey
      })
      .map(x=>{
        (x._1,x._2.firstKey)
      })
      .take(10)
      .foreach(println)
 }
}
