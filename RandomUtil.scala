package org.deep.threshold.util

import scala.util.Random

/**
  * 随机数生成工具类
  * create by zhuy 2019-11-05
  */
object RandomUtil {

  /**
    * 获取0～iMax之间的随机整数
    * @param iMax 随机数最大值
    * @return 随机数
    */
  def getIntRandom(iMax:Int) :Int = {
    Random.nextInt(iMax)
  }

}
