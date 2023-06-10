package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deep.threshold.util.{DFCSVUtil, DFExcelUtil, DFUtil, LogUtil}

object GenBaseGroupCSV {

  var spark:SparkSession = null
  var list:List[String] = List.empty
  var df:DataFrame = null

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("GenBaseGroupCSV")
      .getOrCreate()

    try{
      initCode

      writeResult2CSV
    }catch {
      case e:Exception => LogUtil.logERROR("CSV文件处理失败,失败原因为:",e)
    }

  }


  private def initCode():Unit = {

    df = spark.read.parquet("hdfs://master:9000/dap/projs/threshold/data/source/dat_order_prepare.parquet")
    df.createOrReplaceTempView("dat_order_prepare")

    val dgrList = getDF2List(spark.sql("select adrg from dat_order_prepare where adrg != 'NONE_GROUP' group by adrg"))
    dgrList.toArray.foreach(c => {
      val col = c.asInstanceOf[java.util.HashMap[String,Object]]
      list ++= List(col.get("adrg").toString)
    })

    println(s"待执行主干组列表为：${list}")
    LogUtil.logINFO(s"共[${list.size}]个主干组待执行")
  }

  private def writeResult2CSV() :Unit = {
    var filePath :String = ""

    var cnt:Int = 0
    list.foreach(drgcode => {
      try{
        val dfADRG = spark.sql(s"select adrg,fee,zyts in_days from dat_order_prepare where adrg = '${drgcode}'")
        filePath = s"hdfs://master:9000/dap/projs/threshold/temp/csv/${drgcode}.xlsx"
        DFExcelUtil.saveDF2Excel(dfADRG,filePath)
      }catch {
        case e:Exception => null
      }

      cnt = cnt +1
      if ((cnt % math.floor(list.size/10.0) == 0))  LogUtil.logINFO(s"[主干组 -> ${drgcode}]已完成[${(cnt*100.0/list.size).formatted("%.2f").toString}%]的csv磁盘写入处理")
    })
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
