package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{DFUtil, HDFSUtil, LogUtil, ShellUtil}

import scala.collection.mutable

object DownloadJS {

  private var codeList:List[String] = List.empty
  private val codeMap:java.util.HashMap[String,java.util.HashMap[String,Any]] = new java.util.HashMap[String,java.util.HashMap[String,Any]]
  var spark:SparkSession = null
  var totalOrderCnt:Int = 0    // 总病历份数
  var calcedOrderCnt:Int = 0   // 已完成计算的病历份数
  var totalCodeCnt:Int = 0     // 主干组总个数
  var calcedCodeCnt:Int = 0    // 已完成计算的主干组个数

  // 主干组编码队列
  private [this] val codeQueue:mutable.Queue[String] = mutable.Queue.empty

  // 正在执行的主干组列表
  private [this] var codeSeq:mutable.Seq[String] = mutable.Seq.empty
  private val basePath:String = s"${Dict.HDFSURL}/threshold/data/base/"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("ThresholdService")
      .master("local[1]")
      .getOrCreate()

    try{
      downloadJSON()
    }catch {
      case e:Exception => LogUtil.logERROR("下载阈值表json文件处理失败,失败原因为:",e)
    }
  }

  /**
    * 初始化主干组队列
    */
  private def downloadJSON() :Unit = {
    var df = spark.read.parquet("hdfs://master:9000/threshold/data/threshold_stat.parquet")
    df.createOrReplaceTempView("threshold_stat")
    df = spark.sql("select * from threshold_stat order by `主干组编码` asc")
    val dataList = getDF2List(df)

    dataList.toArray.foreach(c => {
      val map:java.util.HashMap[String,Any] = new java.util.HashMap[String,Any]()
      val col = c.asInstanceOf[java.util.HashMap[String,Object]]
      codeList ++= List(col.get("主干组编码").toString)

      map.put("cnt", col.get("病例份数").toString.toInt)
      map.put("finish", false)
      codeMap.put(col.get("主干组编码").toString, map)
      totalOrderCnt = totalOrderCnt + col.get("病例份数").toString.toInt
      totalCodeCnt = totalCodeCnt +1
    })

    codeList.foreach(code => codeQueue.enqueue(code))
    println(s"待执行主干组列表为：${codeList}")
    LogUtil.logINFO(s"共[${totalCodeCnt}]个主干组待执行,合计病历份数为[${totalOrderCnt}]")


    LogUtil.logINFO(s"DRG各主干组阈值表数据均已生成完毕,开始下载阈值表配置json文件")
    codeList.foreach(code => {
      println(s"hdfs dfs -get /threshold/data/base/${code}/${code.toLowerCase}.json /deep/threshold_json/")
      ShellUtil.exec(s"hdfs dfs -get /threshold/data/base/${code}/${code.toLowerCase}.json /deep/threshold_json/")
    })
//    codeList.foreach(code => println(s"hdfs dfs -get /threshold/data/base/${code}/${code.toLowerCase}.json /deep/threshold_json/"))
    LogUtil.logINFO(s"阈值表配置json文件全部下载成功,阈值表计算处理成功正常退出")
  }



  protected def getDF2List(sDF:DataFrame) :java.util.List[java.util.HashMap[String,Object]] = {
    var fieldMap:Map[String,String]  = Map.empty
    sDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    DFUtil.DF2List(sDF,fieldMap)
  }

}
