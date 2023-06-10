package org.deep.threshold.proc

import java.util

import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{HDFSUtil, LogUtil, ShellUtil}

import scala.util.parsing.json.JSONObject

/**
  * 亚组Kmeans处理类
  */
class SubSetKMeansProc(spark:SparkSession) extends ProcTrait {

  private [this] var cvVaule:Double = 0.00     // 主干组病历费用变异系数值
  private [this] var bContinue:Boolean = true
  private [this] var groupNum:Int = -1         // 亚组个数
  private [this] var subCodeList:List[String] = List.empty

  // 子集码与DRG组编码关系Map
  private [this] var drgsCodeMap:util.HashMap[String,String] = new util.HashMap[String,String]()

  /**
    * 子集聚类处理
    * @param mCode
    */
  def runSubSetKmeans(mCode:String) :Unit = {
    mainCode = mCode

    subFeeDataFilePath = Dict.subFeeDataPath.replace("&maincode",mainCode)
    dataSavePath = Dict.setDataPath.replaceAll("&maincode",mainCode)
    subGroupFeeDF = spark.read.parquet(subFeeDataFilePath)

    // 1.过滤不需要的子集
    filterSubSet

    // 2.将有效子集数据进行合并,并计算其变异系数
    calcAllDataCV

    // 3.Kmeans聚类处理
    if (bContinue) runKmeans
  }

  /**
    * 获取子集码与DRG组编码关系Map
    * @return
    */
  def getDrgsCodeMap() :util.HashMap[String,String] = {
    drgsCodeMap
  }

  /**
    * 按照以下条件过滤子集数据
    * (1)病历份数小于等于2份
    * (2)3~5份且变异系数大于0.5
    * (3)6~9份且变异系数大于0.6
    * (4)10份以上,变异系数大于等于1
    */
  private def filterSubSet() :Unit = {
    var whereSQL:String = "`子集码` not in ("
    // (1) 获取病历份数小于等于2的子集码列表
    df = subGroupFeeDF.filter("(`病历份数` <= 2) " +
      " or (`病历份数` >=3 and `病历份数` <= 5 and `变异系数` > 0.5)" +
      " or (`病历份数` >=6 and `病历份数` <= 9 and `变异系数` > 0.6) " +
      "or (`病历份数` >=10 and `变异系数` >= 1)")
    val codeList:List[String] = List2List(getDFValueList(df,"子集码"))
    LogUtil.logINFO(s"[主干组 -> ${mainCode}] 共有[${codeList.length}]个子集被过滤")

    if(codeList.length > 0){
      // 删除无用的子集数据
      codeList.foreach(code => {
        whereSQL = whereSQL + s"'${code}',"
        HDFSUtil.deleteFile(s"${dataSavePath}${code}.parquet")
      })

      whereSQL = whereSQL.substring(0,whereSQL.length -1) + ")"

      subGroupFeeDF = subGroupFeeDF.where("not ((`病历份数` <= 2) " +
        " or (`病历份数` >=3 and `病历份数` <= 5 and `变异系数` > 0.5)" +
        " or (`病历份数` >=6 and `病历份数` <= 9 and `变异系数` > 0.6) " +
        "or (`病历份数` >=10 and `变异系数` >= 1))")
      subGroupFeeDF.write.mode(SaveMode.Overwrite).parquet(subFeeDataFilePath.replace("sub_fee_data","sub_fee_data_bak"))
      ShellUtil.exec(s"hdfs dfs -rm -r ${subFeeDataFilePath.substring(18)}")
      ShellUtil.exec(s"hdfs dfs -mv ${subFeeDataFilePath.replace("sub_fee_data","sub_fee_data_bak")} ${subFeeDataFilePath.substring(18)}")
    }
  }

  /**
    * 计算主干组所有数据的变异系数
    */
  private def calcAllDataCV() :Unit = {
    subGroupFeeDF = spark.read.parquet(subFeeDataFilePath)
    subCodeList = List2List(getDFValueList(subGroupFeeDF,"子集码"))

    // 无需要参与计算的子集数据
    if (subCodeList.size == 0){
      bContinue =false
      return
    }


    // 所有有效数据进行归类处理
    collectAllData
    if(!bContinue) return

    val cvRet = getDFValueList(df.selectExpr("stddev_pop(`医疗费用`)/avg(`医疗费用`) as `变异系数`").toDF(),"变异系数")
    cvVaule = cvRet.get(0).toDouble

    if (cvVaule <= 0.4)    groupNum = 1
    else if (cvVaule > 0.4 && cvVaule <= 0.6) groupNum = 2
    else if (cvVaule > 0.6 && cvVaule <= 0.8) groupNum = 3
    else groupNum = 4

    LogUtil.logINFO(s"[主干组 -> ${mainCode}] 有效数据计算变异系数,并确定亚组个数为[${groupNum}]个")
  }

  /**
    * 归集所有子集数据到XXX_all.parquet中
    */
  private def collectAllData() :Unit = {

    if (subCodeList.size == 0) {
      LogUtil.logWARN(s"当前主干组[$mainCode]无子集数据,不再继续处理")
      bContinue = false
      return
    }


    subCodeList.foreach(code => {
      df = spark.read.parquet(s"${dataSavePath}${code}.parquet")

      if (HDFSUtil.isParquetFileExist(s"${dataSavePath}${mainCode}_all.parquet")) {
        df.write.mode(SaveMode.Append).parquet(s"${dataSavePath}${mainCode}_all.parquet")
      }else {
        df.write.mode(SaveMode.Overwrite).parquet(s"${dataSavePath}${mainCode}_all.parquet")
      }
    })

    df = spark.read.parquet(s"${dataSavePath}${mainCode}_all.parquet")
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]合并后的总记录数为[${df.count()}]")
  }


  /**
    * 按子集码进行Kmeans聚类
    */
  private def runKmeans() :Unit = {
    // 不细分的直接补编码9
    if (groupNum == 1){
      subCodeList.foreach(code => {
        df = spark.read.parquet(s"${dataSavePath}${code}.parquet")
        df = df.withColumn("`DRG组编码`",functions.lit(s"${mainCode}${Dict.DRG_SUBCODE_9}"))
        df.write.mode(SaveMode.Overwrite).parquet(s"${dataSavePath}${code}_bak.parquet")
        ShellUtil.exec(s"hdfs dfs -rm -r ${(s"${dataSavePath}${code}.parquet").substring(18)}")
        ShellUtil.exec(s"hdfs dfs -mv ${dataSavePath}${code}_bak.parquet ${(s"${dataSavePath}${code}.parquet").substring(18)}")

        drgsCodeMap.put(code,s"${mainCode}9")
      })
    }else {
      val ssKMeans:SSKMeans = new SSKMeans(spark)
      ssKMeans.runSSKmeans(mainCode,groupNum)

      drgsCodeMap = ssKMeans.getDrgsCodeMap()
    }

    var codeMap:Map[String,Any] = Map.empty
    drgsCodeMap.keySet().toArray.foreach(code => {
      val codeStr:String = code.toString
      codeMap ++= Map(codeStr -> drgsCodeMap.get(codeStr))
    })

    HDFSUtil.saveContent2File(Dict.HDFSURL,s"/threshold/data/base/${mainCode}/drgcode.json",s"${JSONObject.apply(codeMap).toString()}")
  }

}
