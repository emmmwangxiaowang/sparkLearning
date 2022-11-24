package demos

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable

/**
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/24
 */
case class Buffer( var total : Long, var cityMap:mutable.Map[String, Long] )

class CityRemarkUDAF extends Aggregator[String, Buffer, String]{
  // 缓冲区初始化
  override def zero: Buffer = {
    Buffer(0, mutable.Map[String, Long]())
  }

  // 更新缓冲区数据
  override def reduce(buff: Buffer, city: String): Buffer = {
    buff.total += 1
    val newCount = buff.cityMap.getOrElse(city, 0L) + 1
    buff.cityMap.update(city, newCount)
    buff
  }

  // 合并缓冲区数据
  override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
    buff1.total += buff2.total

    val map1 = buff1.cityMap
    val map2 = buff2.cityMap

    // 两个Map的合并操作
    //            buff1.cityMap = map1.foldLeft(map2) {
    //                case ( map, (city, cnt) ) => {
    //                    val newCount = map.getOrElse(city, 0L) + cnt
    //                    map.update(city, newCount)
    //                    map
    //                }
    //            }
    map2.foreach{
      case (city , cnt) => {
        val newCount = map1.getOrElse(city, 0L) + cnt
        map1.update(city, newCount)
      }
    }
    buff1.cityMap = map1
    buff1
  }
  // 将统计的结果生成字符串信息
  override def finish(buff: Buffer): String = {
    val remarkList =new StringBuffer()


    val totalcnt = buff.total
    val cityMap = buff.cityMap

    // 降序排列
    val cityCntList = cityMap.toList.sortWith(
      (left, right) => {
        left._2 > right._2
      }
    ).take(2)

    var rsum = 0L
    cityCntList.foreach{
      case ( city, cnt ) => {
        val r = cnt * 100 / totalcnt
        if(remarkList.toString.isEmpty){
          remarkList.append(s"${city} ${r}%")
        }else{
          remarkList.append(s",${city} ${r}%")
        }
        rsum += r
      }
    }
    remarkList.append(s",其他 ${100 - rsum}%").toString

  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}