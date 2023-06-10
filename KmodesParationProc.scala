package org.deep.threshold.memproc

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.deep.threshold.util.LogUtil

/**
  * 基于内存的Kmodes 聚类计算处理
  */
class KmodesParationProc(spark:SparkSession) extends ProcTrait {

  // 主干组数据内存存储Map
  private [this] var dataMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()
  // 子集费用分析表
  private [this] var subFeeMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()
  // 子集码列表
  private [this] var setCodeList:List[String] = List.empty

  private [this] var schema:StructType = null
  private [this] var feeSchema:StructType = null


  /**
    * 内存数据初始化
    * @param dMap  主干组数据
    * @param sfMap 子集费用分析数据
    */
  def dataInit(dMap:util.HashMap[String,util.HashMap[String,Object]],sfMap:util.HashMap[String,util.HashMap[String,Object]]) :Unit = {
    dataMap = dMap
    subFeeMap = sfMap
  }


  def initSchema(dSchema:StructType , fSchema:StructType) :Unit = {
    schema = dSchema
    feeSchema = fSchema
  }

  def runKmodesParationProc(mCode:String) :Unit = {
    mainCode = mCode

    // 获取需计算的子集码列表
    getSetCodeList()

    // Kmodes聚类计算
    kModes()
  }

  def getDataMap() :util.HashMap[String,util.HashMap[String,Object]] = {
    dataMap
  }

  def getSubFeeMap() :util.HashMap[String,util.HashMap[String,Object]] = {
    subFeeMap
  }

  /**
    * 获取病历份数大于10且变异系数大于等于0.5的全部子集
    */
  private def getSetCodeList() :Unit = {

    subFeeMap.keySet().toArray.foreach(code => {
      val setCode = code.toString

      val cv:Double = subFeeMap.get(setCode).get("变异系数").toString.toDouble
      val orderCnt:Long = subFeeMap.get(setCode).get("病历份数").toString.toLong

      if (cv >= 0.5 && orderCnt > 10){
        setCodeList ++= List(setCode)
      }
    })
  }


  /**
    * Kmodes聚类处理
    */
  private def kModes() :Unit = {
    var bRun:Boolean = true
    var calcList:List[String] = setCodeList
    var iLoop:Int = 1

    while (bRun){

      setCodeList = List.empty
      LogUtil.logINFO(s"第[${iLoop}]次Kmodes聚类子集划分处理,共需处理[${calcList.size}]个子集")
      calcList.foreach(set_code => {

        // 取子集码对应的数据信息
        val setData:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()
        dataMap.keySet().toArray.foreach(key => {
          val dataCode = dataMap.get(key.toString).get("set_code").toString
          if (dataCode.equals(set_code)) setData.put(key.toString,dataMap.get(key.toString))
        })

        if(!setData.isEmpty){
          val kModes:KModes = new KModes()
          kModes.runKModels(setData,subFeeMap,mainCode,set_code)
          val reCalList = kModes.getreCalcSubCOdeList()   // 需重新计算的子集列表
          val newSetData = kModes.getMapData()            // 更新完子集码之后的病历数据信息

          setCodeList ++= reCalList
          // 更新病历所属子集信息
          newSetData.keySet().toArray.foreach(key => {
            val newSetCode:String = newSetData.get(key.toString).get("set_code").toString
            dataMap.get(key.toString).put("set_code",newSetCode)
          })
        }
      })

      if (setCodeList.isEmpty){
        LogUtil.logINFO("已无需迭代计算的子集,KModes聚类处理完毕")
        bRun = false
      } else{
        calcList = List.empty
        calcList = setCodeList
      }

      iLoop = iLoop + 1
    }
  }

}
