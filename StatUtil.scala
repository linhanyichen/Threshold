package org.deep.threshold.util

import org.apache.spark.util.StatCounter

object StatUtil {

  /**
    * 计算差异系数
    * @param list 待计算的数据列表
    * @return 差异系数
    */
  def cv(list:List[Double]) :Double = {
    StatCounter(list).stdev / StatCounter(list).mean
  }

  def max(list:List[Double]) :Double = {
    StatCounter(list).max
  }

  def min(list:List[Double]) :Double = {
    StatCounter(list).min
  }

  /**
    * 计算标准差
    * @param list 待计算的数据列表
    * @return 标准差
    */
  def stdev(list:List[Double]) :Double = {
    StatCounter(list).stdev
  }

  /**
    * 计算百分位数
    * @param list 待计算的数据列表
    * @return 百分位数(默认为中位数)
    */
  def median(list:List[Double], pren:Int = 50) :Double = {
    val l = list.sortBy({x:Double => x})
    val len = l.length

    if (len == 0) return 0
    if (len == 1) return list(0)

    //求中位数
    if(pren == 50){
      if(len %2 == 0 ){
        (l(len/2) + l(len/2 -1)) /2.0
      }else {
        l(len/2)
      }
    }else{
      val index:Int = math.ceil(len * pren / 100.0).toInt

      //不是整数,取index对应下标值
      if(index != (len * pren / 100.0).toInt){
        l(index-1)
      }else{//是整数,取index和index+1对应下标值平均数
        (l(index-1) + l(index)) /2.0
      }
    }
  }

  /**
    * 计算平均数
    * @param list 待计算的数据列表
    * @return 平均数据
    */
  def avg(list:List[Double]):Double = {
    StatCounter(list).mean
  }
}
