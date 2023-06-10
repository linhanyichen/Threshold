package org.deep.threshold.proc

import java.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{LogUtil, ShellUtil}

/**
  * KModels 聚类算法进行子集划分处理类
  */
class KModelsParationProc(spark:SparkSession) extends ProcTrait {
  private [this] var subCodeList:util.List[String] = new util.LinkedList[String]()

  // 存放因所有属性向量相同而无需进行K-models聚类的子集码列表
  private [this] var unComputedList:List[String] = List.empty
  // K-models聚类产生的子集列表
  private [this] val kSubCodeList:util.List[String] = new util.LinkedList[String]()

  // K-models聚类过程中被分解需删除的子集码列表
  private [this] val delSubCodeList:util.List[String] = new util.LinkedList[String]()

  def runKModelsParation(mCode:String) :Unit = {
    mainCode = mCode
    subFeeDataFilePath = Dict.subFeeDataPath.replace("&maincode",mainCode)
    var bRun:Boolean = true

    // 1.获取变异系数大于0.5的子集码列表
    getSubCodeList()

    var loopCnt:Int = 1
    while (bRun){
      subCodeList.toArray.foreach(subCode => {
        val filePath :String = Dict.setDataPath.replace("&maincode" , mainCode)+s"${subCode.toString}.parquet"
        val df:DataFrame = spark.read.parquet(filePath)

        val kModels:KModels = new KModels(spark)
        kModels.runKModels(df,mainCode,subCode.toString)

        kModels.getreCalcSubCOdeList().foreach(code => kSubCodeList.add(code))
        if (kModels.bDelFromSubFee()) delSubCodeList.add(subCode.toString)
      })

      // 更新子集费用分析表,删除已不再使用的子集数据信息
      if(delSubCodeList.size() > 0){
        LogUtil.logINFO("更新子集费用分析表数据,删除已被分解的子集数据信息")
        var whereSQL :String = "`子集码` not in ("
        delSubCodeList.toArray.foreach(code => whereSQL = whereSQL + s"'${code.toString}' ," )
        whereSQL = whereSQL.substring(0,whereSQL.length-1) + ")"

        var subFeeDF:DataFrame = spark.read.parquet(subFeeDataFilePath)
        subFeeDF = subFeeDF.where(whereSQL).na.fill(0.0).repartition(2)

        subFeeDF.write.mode(SaveMode.Overwrite).parquet(subFeeDataFilePath.replace("sub_fee_data","sub_fee_data_bak"))
        ShellUtil.exec(s"hdfs dfs -rm -r ${subFeeDataFilePath.substring(18)}")
        ShellUtil.exec(s"hdfs dfs -mv ${subFeeDataFilePath.replace("sub_fee_data","sub_fee_data_bak")} ${subFeeDataFilePath.substring(18)}")
        delSubCodeList.clear()

        LogUtil.logINFO("子集费用分析表数据更新完毕")
      }

      // 没有需要再聚类的子集,则退出循环
      if (kSubCodeList.size() == 0) bRun = false
      else {
        LogUtil.logINFO(s"=============[主干组 -> ${mainCode}][第${loopCnt}次迭代]共[${kSubCodeList.size()}]个子集需进行Kmodels再聚类=============")
        subCodeList.clear()
        kSubCodeList.toArray.foreach(code => subCodeList.add(code.toString))
        loopCnt = loopCnt + 1
        kSubCodeList.clear()
      }
    }// end for while
  }


  /**
    * 获取子集码列表
    */
  private def getSubCodeList() :Unit = {
    df = spark.read.parquet(subFeeDataFilePath).where("`变异系数` >= 0.5 and `病历份数` > 10").toDF
    subCodeList = getDFValueList(df, "子集码")
  }

}
