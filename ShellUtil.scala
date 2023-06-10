package org.deep.threshold.util

import java.io.{BufferedReader, InputStreamReader}


import scala.util.control.Breaks

object ShellUtil {

  var driverId:String = ""

  /**
    * 执行shell脚本完成相关操作
    * @param cmdStr   shell脚本
    * @param succMsg  执行成功对应的信息
    * @return
    */
  def exec(cmdStr:String,succMsg:String="") :String = {
    var bSuccess :Boolean = true
    var sb:StringBuilder = new StringBuilder

    var exitValue :Int = -9
    val loop = new Breaks

    val runTime = sys.runtime
    val proc = runTime.exec(cmdStr)

    sb.append("")
    try{

      val inputStr = new BufferedReader(new InputStreamReader(proc.getInputStream))
      var msg:String = inputStr.readLine()

      while (msg != null){
//        LogUtil.logDEBUG(s"OutPutInfo::${msg}")
        msg = inputStr.readLine()
      }

      /**获取错误信息*/
      val error = new BufferedReader(new InputStreamReader(proc.getErrorStream()))
      msg = error.readLine
      while (msg != null){
//        LogUtil.logDEBUG(s"LogInfo::${msg}")
        msg = error.readLine
      }

      exitValue = proc.waitFor()
    }catch{
      case e:Exception => {
        exitValue = -1
        bSuccess = false
        LogUtil.logERROR(s"执行Shell脚本失败,失败原因为：",e)
      }
    }

    /**提交后标志字段仍为初始值*/
    if(exitValue == -9){
      loop.breakable({
        var iloop:Int = 0
        while(true){
          exitValue = proc.exitValue()
          if(exitValue != -9){
            loop.break()
          }

          /**3分钟如果还没执行完,就Kill掉,防止出现类似死循环类的处理*/
          if(iloop > 180 && exitValue == -9){
            proc.destroyForcibly()
            LogUtil.logWARN("强制关闭进程")
            loop.break()
          }

          Thread.sleep(1000)
          iloop += 1
        }
      })
    }

    /**成功时返回提示信息*/
    if(bSuccess){
      sb.clear()
      sb.append(s"${succMsg}")
    }

    sb.toString()
  }

}
