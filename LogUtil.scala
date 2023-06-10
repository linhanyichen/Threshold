package org.deep.threshold.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * 日志信息工具类
  * create by zhuy 2019-10-28
  */
object LogUtil {

  private [this] val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private [this] val calendar:Calendar = Calendar.getInstance()

  /**
    * 输出日志信息
    * @param logInfo 日志内容
    * @param logLevel 日志级别,默认为DEBUG级别
    * @param ex 当前处理Throwable,默认为null
    */
  private[this] def logMessage(logInfo:String, logLevel:String = "DEBUG", ex:java.lang.Throwable = null) : Unit = {
    var logMsg = s"${getCurrMSDateTime()} ${logLevel} ${logInfo}"
    if(ex != null){
      // 输出写日志位置信息
      val msg = getStackString(ex)
      logMsg = s"${logMsg} ${msg}"
    }

    println(logMsg)
  }


  /**
    * 输出DEBUG级别日志信息
    * @param logInfo 日志内容
    * @param ex 当前处理Throwable,默认为null
    */
  def logDEBUG(logInfo:String, ex:java.lang.Throwable = null) :Unit = {
      logMessage(logInfo,"DEBUG",ex)
  }

  /**
    * 输出INFO级别日志信息
    * @param logInfo 日志内容
    * @param ex 当前处理Throwable,默认为null
    */
  def logINFO(logInfo:String, ex:java.lang.Throwable = null) :Unit = {
    logMessage(logInfo,"INFO",ex)
  }

  /**
    * 输出WARN级别日志信息
    * @param logInfo 日志内容
    * @param ex 当前处理Throwable,默认为null
    */
  def logWARN(logInfo:String, ex:java.lang.Throwable = null) :Unit = {
    logMessage(logInfo,"WARN",ex)
  }

  /**
    * 输出ERROR级别日志信息
    * @param logInfo 日志内容
    * @param ex 当前处理Throwable,默认为null
    */
  def logERROR(logInfo:String, ex:java.lang.Throwable = null) :Unit = {
    logMessage(logInfo,"ERROR",ex)
  }


  /**
    * 获取异常堆栈信息
    * @param ex 异常
    * @return 堆栈信息
    */
  private def getStackString(ex : Throwable): String = {
    var e = ex
    var msg = ""

    while (e != null) {
      val sts = e.getStackTrace
      msg += e.getMessage + "\n"
      for (st <- sts) {
        msg += getCurrMSDateTime() + " " + st.toString + "\n"
      }
      e = e.getCause
    }

    msg
  }


  def getCurrMSDateTime():String = {
    val sdf:SimpleDateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")
    val current:String = sdf.format(Calendar.getInstance.getTime)

    current
  }

}
