package demo

/**
 *
 * @Author: 王航
 * @Email: 954544828@qq.com
 * @Date: 2022/11/19
 *
**/
/*
提供一个隐式转换类型操作
若使用隐式转换函数必须使用单例类{object类}
 */
object  SortFloat{
  // 提供隐式转换函数进行自定义排序操作
  implicit val sortFloat = new Ordering[Float]{
    override def compare(x: Float, y: Float): Int = {
      if(Ordering.Float.gt(x,y)){
        -1
      }else{
        1
      }
    }
  }


}
