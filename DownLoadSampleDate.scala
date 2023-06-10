package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deep.threshold.util.{DFExcelUtil, LogUtil}

object DownLoadSampleDate {

  var mainCode:String = ""
  var dfRet:DataFrame = null
  var spark:SparkSession = null

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("DownLoadSampleDate")
      .getOrCreate()

    val codeIndex:Int = args.indexOf("--code")

    if (codeIndex > -1) {
      mainCode = args(codeIndex + 1)
    }

    dealSampleData
    writeResult2Excel()
  }

  private def dealSampleData():Unit = {
    val dfStat:DataFrame = spark.read.parquet(s"hdfs://master:9000/threshold/data/base/${mainCode}/sub_fee_data.parquet")
    val df:DataFrame = spark.read.parquet(s"hdfs://master:9000/threshold/data/base/${mainCode}/${mainCode}_all.parquet")

    dfStat.createOrReplaceTempView("sub_fee_data")
    df.createOrReplaceTempView(s"${mainCode}_all")

    dfRet = spark.sql(s"select t.*,s.`DRG组编码` from ${mainCode}_all t left join sub_fee_data s on t.`子集码` = s.`子集码`")
  }

  private def writeResult2Excel() :Unit = {
    LogUtil.logINFO(s"${mainCode}主干组样本数据结果写入路径::/threshold/data/result/${mainCode}_sample.xlsx")
    DFExcelUtil.saveDF2Excel(dfRet,s"hdfs://master:9000/threshold/data/result/${mainCode}_sample.xlsx")
  }

}
