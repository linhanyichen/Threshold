package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.deep.threshold.util.{DFExcelUtil, DFUtil, HDFSUtil, LogUtil}

/**
  * 阈值表统计处理
  */
object ThresholdStat {

  var spark:SparkSession = null
  var schema:StructType = StructType(List(
    StructField("主干组编码", StringType, nullable = true),
    StructField("病例份数", IntegerType, nullable = true),
    StructField("有效病例份数", IntegerType, nullable = true),
    StructField("亚组个数", IntegerType, nullable = true),
    StructField("子集个数", IntegerType, nullable = true),
    StructField("是否存在阈值表", StringType, nullable = true),
    StructField("变异系数", DoubleType, nullable = true)
  ))

  val statMap:java.util.HashMap[String,java.util.HashMap[String,Any]] = new java.util.HashMap[String,java.util.HashMap[String,Any]]()
  var codeList:List[String] = List.empty

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("ThresholdStat")
      //      .master("local[*]")
      .getOrCreate()

    initStat()

    thresholdStat()

    saveTresholdStat()
  }

  private def initStat() :Unit = {
    val df = spark.read.parquet("hdfs://master:9000/threshold/data/threshold_stat.parquet")

    val statList = getDF2List(df.orderBy("`主干组编码`").toDF())
    for (i<-0 to statList.size()-1){
      val row = statList.get(i)

      val drgCode:String = row.get("主干组编码").toString
      val orderCnt:Int = row.get("病例份数").toString.toInt
      val data:java.util.HashMap[String,Any] = new java.util.HashMap[String,Any]()
      data.put("病例份数", orderCnt)
      data.put("有效病例份数", orderCnt)
      data.put("亚组个数", orderCnt)

      codeList ++= List(drgCode)
      statMap.put(drgCode, data)
    }

    LogUtil.logINFO("统计数据初始化完毕")
  }

  private def thresholdStat() :Unit = {

    val basePath:String = "/threshold/data/base/"
    var loopCnt:Int = 1

    codeList.foreach(code => {

      val noThresPath:String = basePath + code + "/nothreshold.ini"
      val finishPath:String =  basePath + code + "/finished.ini"

      val subFeePath:String = basePath + code + "/sub_fee_data.parquet"
      val allDataPath:String = basePath + code + s"/${code}_all.parquet"

      var groupNum:Int = 1  // 亚组个数,默认为1
      var enorderCnt:Int = 0 // 有效病历数
      var setCnt:Int = 1
      var cv:Double = 0.0
      var hasTable:String = "否"   // 是否存在阈值表


      if (HDFSUtil.isIniFileExist("hdfs://master:9000"+noThresPath)){
        groupNum = 1
        enorderCnt = statMap.get(code).get("病例份数").toString.toInt

        val allDF = spark.read.parquet(s"hdfs://master:9000/threshold/data/${code}/trunk_group_data.parquet")
        allDF.createOrReplaceTempView(s"${code}_all")
        val cvRet = getDF2List(spark.sql(s"select stddev_pop(`医疗费用`)/avg(`医疗费用`) as `变异系数` from ${code}_all"))
        cv = cvRet.get(0).get("变异系数").toString.toDouble

      }else if (HDFSUtil.isIniFileExist("hdfs://master:9000"+finishPath)){
        val df =  spark.read.parquet("hdfs://master:9000" + subFeePath)
        df.createOrReplaceTempView(s"${code}")
        val statRet = getDF2List(spark.sql(s"select sum(`病历份数`) `有效病例份数`, count(distinct `DRG组编码`) groupNum, count(1) `子集个数` from ${code}"))
        enorderCnt = statRet.get(0).get("有效病例份数").toString.toInt
        setCnt = statRet.get(0).get("子集个数").toString.toInt
        groupNum = statRet.get(0).get("groupNum").toString.toInt

        val allDF = spark.read.parquet("hdfs://master:9000" + allDataPath)
        allDF.createOrReplaceTempView(s"${code}_all")
        val cvRet = getDF2List(spark.sql(s"select stddev_pop(`医疗费用`)/avg(`医疗费用`) as `变异系数` from ${code}_all"))
        cv = cvRet.get(0).get("变异系数").toString.toDouble

        hasTable = if (groupNum ==1) "否" else "是"
      } else{
        LogUtil.logWARN(s"[${code}]主干组无法进行数据信息统计")
        setCnt = 0
        groupNum = 0
      }

      statMap.get(code).put("有效病例份数", enorderCnt)
      statMap.get(code).put("子集个数", setCnt)
      statMap.get(code).put("亚组个数", groupNum)
      statMap.get(code).put("是否存在阈值表", hasTable)
      statMap.get(code).put("变异系数", cv)
      LogUtil.logINFO(s"已完成第[${loopCnt}]个主干组数据统计")
      loopCnt = loopCnt + 1
    })
  }

  /**
    * 保存阈值表统计结果
    */
  private def saveTresholdStat() :Unit = {
    var rowSeq:Seq[Row] = Seq.empty

    codeList.foreach(code => {
      val data:Seq[Any] = Seq(code,statMap.get(code).get("病例份数").toString.toInt,
        statMap.get(code).get("有效病例份数").toString.toInt,
        statMap.get(code).get("亚组个数").toString.toInt,
        statMap.get(code).get("子集个数").toString.toInt,
        statMap.get(code).get("是否存在阈值表").toString,
        statMap.get(code).get("变异系数").toString.toDouble)
      val row:Row = Row.fromSeq(data)
      rowSeq ++= Seq(row)
    })

    val rdd = spark.sparkContext.parallelize(rowSeq)
    val df = spark.sqlContext.createDataFrame(rdd, schema)

    df.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/threshold/data/threshold_stat_final.parquet")
    DFExcelUtil.saveDF2Excel(df,"hdfs://master:9000/threshold/data/threshold_stat.xlsx")
    LogUtil.logINFO("最终统计结果数据保存完毕,写入路径：hdfs://master:9000/threshold/data/threshold_stat.xlsx")
  }


  /**
    * 将DF转成List[Map]
    * @param sDF
    * @return
    */
  protected def getDF2List(sDF:DataFrame) :java.util.List[java.util.HashMap[String,Object]] = {
    var fieldMap:Map[String,String]  = Map.empty
    sDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    DFUtil.DF2List(sDF,fieldMap)
  }

}
