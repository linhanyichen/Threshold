package org.deep.threshold.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 获取各主干组有效病历信息所在亚组信息
  */
object GetDRGsResult {

  var spark:SparkSession = null
  var mainCode:String = ""

  def main(args: Array[String]): Unit = {

    val codeIndex:Int = args.indexOf("--code")

    if (codeIndex > -1) {
      mainCode = args(codeIndex + 1)
    }

    val sparkConf = new SparkConf().setAppName(s"GetDRGsResult-${mainCode}")
    /*.set("spark.scheduler.mode", "FAIR")*/
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark = SparkSession.builder()
      .config(sparkConf)
      //                        .master("local[*]")
      .getOrCreate()

    getResult

  }

  private def getResult() :Unit = {
    var df:DataFrame = null
    val subFeeData:DataFrame = spark.read.parquet(s"hdfs://master:9000/threshold/data/base/${mainCode}/sub_fee_data.parquet")
    subFeeData.createOrReplaceTempView("sub_fee_data")

    val fileList = HDFSUtil.getParquetFileList(s"hdfs://master:9000/threshold/data/base/${mainCode}")
    fileList.foreach(file => {
      // 取子集数据
      if(!file.contains(s"${mainCode}_all") && !file.contains("sub_fee_data")){

        if (df == null) df = spark.read.parquet(file)
        else {
          val dfTmp = spark.read.parquet(file)
          df = df.unionAll(dfTmp)
        }
      }
    })


    df.createOrReplaceTempView("threshold_data")
    val dfResult = spark.sql("select t.a1,t.a2,t.d1,t.d2,t.g,t.r,t.s,t.u,t.`医疗费用`,s.`DRG组编码` from threshold_data t left join sub_fee_data s on t.`子集码` = s.`子集码`")

    DFExcelUtil.saveDF2Excel(dfResult,s"hdfs://master:9000/threshold/data/base/${mainCode}/${mainCode}.xlsx")
  }

}
