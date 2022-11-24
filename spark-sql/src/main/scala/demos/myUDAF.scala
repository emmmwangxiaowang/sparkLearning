package demos

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

import java.text.DecimalFormat


case class CityRemark(cityName:String,cityRadio:Double){
  val f = new DecimalFormat("0.00%")
  //北京21.2%，天津13.2%，其他65.6%
  override def toString:String = s"$cityName:${f.format(cityRadio.abs)}"
}
/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/24
 */
class myUDAF extends UserDefinedAggregateFunction {
  // 输入数据类型
  override def inputSchema: StructType = {
    //  输入类型是一个数组,数组中的元素是一个 cityName
    StructType(
      Array(
        StructField("city",StringType)
      )
    )
  }

  // 缓冲区类型
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("map",MapType(StringType,LongType)),
        StructField("total",LongType)
      )
    )
  }

  // 输出数据类型
  override def dataType: DataType = StringType

  // 这个函数是否是 稳定的 -- 相同输入 能否得到 相同输出
  override def deterministic: Boolean = true

  // 缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=Map[String,Long]()
    buffer(1)=0L
  }

  // 缓冲区 数据 具体操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      case Row(cityName:String)=>
        // 总的点击量+1
        // 因为缓冲区不保存类型(为 Any类型), 所以通过 buffer.getX 转成对应的类型进行操作,之后再回写到 buffer
        buffer(1)=buffer.getLong(1)+1L

        // 城市点击量+1
        // 获取  缓冲区中的  map
        val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
        // 将map  中对应的城市 count+1,没有就创建
        buffer(0)=map+(cityName->(map.getOrElse(cityName,0L)+1L))

      case _=>
    }

  }

  // 分区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)

    val count1: Long = buffer1.getLong(1)
    val count2: Long = buffer2.getLong(1)

    // city 总数
    buffer1(1)=count1+count2

    // map 聚合
    buffer1(0) = map1.foldLeft(map2) {
      case (map, (cityName, count)) =>
        map + (cityName -> (map.getOrElse(cityName, 0L) + count))
    }
  }

  // 最终操作
  override def evaluate(buffer: Row): String = {
    val cityAndCount = buffer.getMap[String,Long](0)
    val total: Long = buffer.getLong(1)

    val top2: List[(String, Long)] = cityAndCount.toList.sortBy(-_._2).take(2)
    var cityRemarks: List[CityRemark] = top2.map {
      case (cityName, count) => CityRemark(cityName, count.toDouble/ total)
    }
    cityRemarks :+=CityRemark("其他",cityRemarks.foldLeft(1D)(_-_.cityRadio))
    cityRemarks.mkString(",")
  }

}

