package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{DFExcelUtil, DFUtil, HDFSUtil, LogUtil}

import scala.collection.mutable

object ThresholdDataStatAnalysis {

  private var codeList:List[String] = List.empty
  var spark:SparkSession = null
  val allDataPath = s"hdfs://master:9000/threshold/data/base/all_drg_data_analysis.parquet"

  // 正在执行的主干组列表
  private [this] var codeSeq:mutable.Seq[String] = mutable.Seq.empty
  private val basePath:String = s"${Dict.HDFSURL}/threshold/data/base/"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("DataStatAnalysis")
      .getOrCreate()

    try{
      initCode

      statAllData

      writeResult2Excel
    }catch {
      case e:Exception => LogUtil.logERROR("阈值表数据分析处理失败,失败原因为:",e)
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
      var df:DataFrame = null
      val statFilePath = s"hdfs://master:9000/threshold/data/base/${code}/${code}_all.parquet"
      if (HDFSUtil.isParquetFileExist(statFilePath)){
        df = spark.read.parquet(statFilePath)
        df.createOrReplaceTempView(s"${code}_all")
       // df.printSchema()
        val df1 = spark.read.parquet(s"hdfs://master:9000/threshold/data/base/${code}/sub_fee_data.parquet")
        df1.createOrReplaceTempView(s"${code}_sub_fee_data")
      //  df1.printSchema()

        df = spark.sql(s"select t.*,s.`DRG组编码` from ${code}_all t left join ${code}_sub_fee_data s on t.`子集码` = s.`子集码`")
      }else{
        df = spark.read.parquet(s"hdfs://master:9000/threshold/data/${code}/dat_order.parquet")
        df = df.withColumn("DRG组编码", functions.lit(s"${code}9"))
      }

      // 汇总统计各DRG组费用均值、中位数及变异系数
      df.createOrReplaceTempView(s"${code}_all")
      df = spark.sql(s"select `DRG组编码`,count(1) `组内病例数`, max(`医疗费用`) `费用最大值`, min(`医疗费用`) `费用最小值`,avg(`医疗费用`) `费用均值`,percentile(`医疗费用`,0.5) `费用中位数`,stddev_pop(`医疗费用`) `标准差`, stddev_pop(`医疗费用`)/avg(`医疗费用`) `变异系数` from ${code}_all group by `DRG组编码`")

      // 追加
      if (HDFSUtil.isParquetFileExist(allDataPath)){
        df.write.mode(SaveMode.Append).parquet(allDataPath)
      }
      // 覆写
      else {
        df.write.mode(SaveMode.Overwrite).parquet(allDataPath)
      }

      LogUtil.logINFO(s"主干组[${code}]数据汇总分析完毕")
    })
  }

  /**
    * 将结果写入excel文件
    */
  private def writeResult2Excel() :Unit = {
    val df = spark.read.parquet(allDataPath)
    df.createOrReplaceTempView("threshold_data_analysis")
    val dfRet = spark.sql("select * from threshold_data_analysis")
    LogUtil.logINFO("各DRG组数据分析指标处理完毕,结果写入路径::/threshold/data/base/threshold_data_analysis.xlsx")
    DFExcelUtil.saveDF2Excel(dfRet,"hdfs://master:9000/threshold/data/base/threshold_data_analysis.xlsx")
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
