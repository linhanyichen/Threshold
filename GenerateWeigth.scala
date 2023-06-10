package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{DFExcelUtil, DFUtil, HDFSUtil, LogUtil, ShellUtil}

import scala.collection.mutable

/**
  * 各DRG组权重值生成处理类
  * create by zhuy 2020-02-02
  */
object GenerateWeigth {

  private var codeList:List[String] = List.empty
  var spark:SparkSession = null
  val allDataPath = s"hdfs://master:9000/threshold/data/base/all_sub_drg_data.parquet"

  // 正在执行的主干组列表
  private [this] var codeSeq:mutable.Seq[String] = mutable.Seq.empty
  private val basePath:String = s"${Dict.HDFSURL}/threshold/data/base/"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("GenerateWeigth")
      .getOrCreate()

    try{
      initCode

      statAllData

      calcDRGWeigth
    }catch {
      case e:Exception => LogUtil.logERROR("阈值表生成处理失败,失败原因为:",e)
    }
  }


  /**
    * 初始化主干组队列
    */
  private def initCode() :Unit = {
    var df = spark.read.parquet("hdfs://master:9000/threshold/data/threshold_stat.parquet")
    df.createOrReplaceTempView("threshold_stat")
    df = spark.sql("select * from threshold_stat order by `主干组编码` asc")
    val dataList = getDF2List(df)

    dataList.toArray.foreach(c => {
      val col = c.asInstanceOf[java.util.HashMap[String,Object]]
      if(!(col.get("主干组编码").toString).equals("NONE_GROUP")) {
        codeList ++= List(col.get("主干组编码").toString)
      }
    })

    LogUtil.logINFO(s"共[${codeList.size}]个主干组待处理")
  }

  private def statAllData() :Unit = {

    codeList.foreach(code => {
      val statFilePath = s"hdfs://master:9000/threshold/data/base/${code}/sub_fee_data.parquet"

      var df = spark.read.parquet(statFilePath)
      df.createOrReplaceTempView(s"${code}_sub_fee_data")

      // 无子集划分的需补齐子集码
      if(spark.sql(s"select `DRG组编码`,count(`DRG组编码`) cnt from ${code}_sub_fee_data group by `DRG组编码`").count() == 1) {
        df = df.drop("DRG组编码")
        df = df.withColumn("DRG组编码", functions.lit(s"${code}9"))
      }

      // 汇总统计各DRG组病历份数和住院总费用
      df.createOrReplaceTempView(s"${code}_sub_fee_data")
      df = spark.sql(s"select `DRG组编码`,sum(`病历份数`) `病历份数`,sum(`费用均值`*`病历份数`) `住院总费用` from ${code}_sub_fee_data group by `DRG组编码`")

      // 追加
      if (HDFSUtil.isParquetFileExist(allDataPath)){
        df.write.mode(SaveMode.Append).parquet(allDataPath)
      }
      // 覆写
      else {
        df.write.mode(SaveMode.Overwrite).parquet(allDataPath)
      }

      LogUtil.logINFO(s"主干组[${code}]数据汇总完毕")
    })
  }

  /**
    * 计算各DRG组权重值
    */
  private def calcDRGWeigth() :Unit = {
    var dfRet = spark.read.parquet(allDataPath)
    dfRet = dfRet.withColumn("total_fee", functions.sum("住院总费用").over())
    dfRet = dfRet.withColumn("total_order", functions.sum("病历份数").over())

    dfRet.createOrReplaceTempView("ADRG_all_sub_drg_data")
    dfRet = spark.sql("select *,`住院总费用`/`病历份数` drg_fee_avg,total_fee/total_order all_fee_avg from ADRG_all_sub_drg_data")

    dfRet.createOrReplaceTempView("ADRG_all_sub_drg_data")
    dfRet = spark.sql("select *,drg_fee_avg/all_fee_avg weigth from ADRG_all_sub_drg_data")

    LogUtil.logINFO("各DRG组权重计算处理完毕,结果写入路径::/threshold/data/base/adrg_weigth.xlsx")
    DFExcelUtil.saveDF2Excel(dfRet,"hdfs://master:9000/threshold/data/base/adrg_weigth.xlsx")

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
