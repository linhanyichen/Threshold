package org.deep.threshold.util

import java.security.MessageDigest
import java.util.UUID

/**
  * 子集码生成工具类
  */
object SubCodeUtil {

  /**
    * 获取子集码
    * @return
    */
  def getSubCode() :String = {
    var subCode = UUID.randomUUID().toString
    subCode = subCode.replace("-","") + LogUtil.getCurrMSDateTime()

    val digest = MessageDigest.getInstance("MD5")
    subCode = digest.digest(subCode.getBytes).map("%02x".format(_)).mkString

    subCode
  }
}
