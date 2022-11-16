package demo

/**
 *  这个类是作为存储数据和排序使用
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/15
 */
case class Girl(faceValue:Int,age:Int)

/*
提供一个隐式转换类型操作
若使用隐式转换函数必须使用单例类{object类}
 */

object  MyOrdering{
  // 提供隐式转换函数进行自定义排序操作
  implicit val girlOrdering = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue!=y.faceValue) {
        x.faceValue-y.faceValue
      }else{
        y.age-x.age
      }
    }
  }


}
