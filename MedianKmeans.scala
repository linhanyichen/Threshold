package org.deep.threshold.memproc

import java.util

import org.apache.spark.sql.DataFrame
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.LogUtil

/**
  * 子集费用中位数Kmeans聚类处理(内存版)
  * create by zhuy 2019-11-20
  */
class MedianKmeans {
  private [this] var mainCode:String = ""
  private [this] var centerNumber:Int = _
  private [this] val drgsCodeMap:util.HashMap[String,String] = new util.HashMap[String,String]()  // 子集亚组编码关系map,key-子集码,value-亚组编码
  private [this] val sampleMap:util.HashMap[Int,String] = new util.HashMap[Int,String]()   // 存放样本数据下标与子集码对应关系

  private [this] var centers:Array[Double] = null
  private [this] var samples:Array[Double] = null         // 样本数据


  // 亚组编码列表
  private [this] val drgSubCodeList:List[String] = List(Dict.DRG_SUBCODE_5,Dict.DRG_SUBCODE_3,Dict.DRG_SUBCODE_2,Dict.DRG_SUBCODE_1)

  private [this] var maxMidFee:Double = Double.MinValue
  private [this] var minMidFee:Double = Double.MaxValue

  private [this] var medianDataMap:util.HashMap[String,Double] = new util.HashMap[String,Double]()

  /**
    * 按子集进行亚组聚类
    * @param mCode   主干组编码
    * @param cNumber 聚类中心点个数
    */
  def runMedianKmeans(mCode:String,cNumber:Int,dataMap:util.HashMap[String,Double]) :Unit = {
    mainCode = mCode
    centerNumber = cNumber
    medianDataMap = dataMap

    LogUtil.logDEBUG(s"开始对[$mCode]主干组数据按子集中位数进行Kmeans聚类处理,聚类中心点个数为[${centerNumber}]个")

    centers = new Array[Double](centerNumber)

    // 1.生成样本数据
    getSamples

    // 2.初始化中心点
    initCenter

    // 3.进行K-means聚类计算
    kmeans()

    // 4.生成DRG亚组编码
    genDRGSubCode

  }

  /**
    * 获取子集码与DRG组编码关系Map
    * @return
    */
  def getDrgsCodeMap() :util.HashMap[String,String] = {
    drgsCodeMap
  }

  /**
    * 生成样本数据
    */
  private def getSamples() :Unit = {

    LogUtil.logDEBUG(s"待处理的数据样本个数为[${medianDataMap.keySet().size}]个")

    var iLoop:Int = 0
    samples = medianDataMap.keySet().toArray.map(key => {
      val code:String = key.toString
      val midFee:Double = medianDataMap.get(code)

      if (maxMidFee < midFee) maxMidFee = midFee
      if (minMidFee > midFee) minMidFee = midFee

      sampleMap.put(iLoop,code)
      iLoop= iLoop + 1
      midFee
    })

    LogUtil.logDEBUG(s"中位数最大值为[${maxMidFee}],最小值为[${minMidFee}]")
  }

  /**
    * 初始化中心点
    */
  private def initCenter() :Unit = {
    /**
      * 设定分组个数为A,则第B个亚组初始聚类中心点值为
      * 最小中位数 + (最大中位数 - 最小中位数)/(1+A)*B
      */
    for (i<-1 to centers.length) centers.update(i-1,minMidFee+(maxMidFee-minMidFee)/(centerNumber+1) * i)
    LogUtil.logINFO(s"[主干组 -> ${mainCode}] SSKmeans 初始聚类中心点为::${centers.toList}")
  }

  /**
    * 执行Kmeans聚类
    */
  private def kmeans() :Unit = {

    var run:Boolean = true

    var loopCnt:Int = 1
    var newCenters = Array[Double]()
    while (run){

      val cluster = samples.groupBy(v => closesetCenter(v))
      newCenters = centers.map(oldCenter => {
        cluster.get(oldCenter) match {
          case Some(pointsInThisCluster) => genNewCenter(pointsInThisCluster)
          case None => -1
        }
      })

      if (isNewCenterLegal(newCenters)){
        LogUtil.logINFO(s"子集Kmeans聚类处理结束,共迭代[$loopCnt]次")
        run = false
      }

      LogUtil.logDEBUG(s"第[${loopCnt}]次Kmeans聚类结束,产生的新中心点为[${newCenters.toList}]")
      // 更新中心点
      var cIndex:Int = 0
      newCenters.foreach(c => {
        if (c > 0) {
          centers.update(cIndex,c)
          cIndex = cIndex + 1
        }
      })

      // 当存在聚类中心点无对应病历时,则减少分组个数
      if (cIndex < centerNumber) {
        centers = centers.dropRight(centerNumber-cIndex)
        centerNumber = cIndex
      }

      loopCnt = loopCnt + 1
    }

    // 中心点进行升序排序
    centers = centers.sorted
  }

  /**
    * 计算距离
    * @param A 中心点
    * @param B 样本点
    * @return A、B点距离
    */
  private def dataDis(A:Double, B:Double) :Double = {
    math.abs(A-B)
  }

  /**
    * 计算样本点所属的聚类中心
    * @param v 样本点
    * @return 聚类中心点
    */
  private def closesetCenter(v:Double) :Double = {
    centers.reduceLeft((c1,c2) => {
      if(dataDis(c1,v) < dataDis(c2,v)) c1 else c2
    })
  }

  /**
    * 根据聚类结果计算新的中心点
    * @param sample
    * @return
    */
  private def genNewCenter(sample:Array[Double]) :Double ={
    // 新中心点为中位数均值
    sample.reduceLeft((v1,v2) => v1+v2) / sample.length
  }

  /**
    * 新旧中心点距离是否大于0.01
    * @param newCenters
    * @return
    */
  private def isNewCenterLegal(newCenters:Array[Double]) :Boolean = {
    for (i<-0 to newCenters.length -1){
      if (dataDis(centers(i),newCenters(i)) > 0.01 && (newCenters(i) > 0)) return false
    }

    true
  }

  /**
    * 根据聚类结果生成亚组编码
    */
  private def genDRGSubCode() :Unit = {
    var closetCenter:Double = 0.00

    for(i<-0 to samples.length -1){
      closetCenter = centers.reduceLeft((c1,c2) => if(dataDis(c1,samples(i)) < dataDis(c2,samples(i))) c1 else c2 )
      val index:Int = centers.indexOf(closetCenter)

      var drgSubCode:String = s"${mainCode}${drgSubCodeList(index)}"
      // 当仅有两组时，取值分别为5,2
      if (centers.length == 2 && index ==1)  drgSubCode = s"${mainCode}${drgSubCodeList(index+1)}"
      drgsCodeMap.put(sampleMap.get(i),drgSubCode)
    }

    LogUtil.logINFO(s"[主干组 -> ${mainCode}] 各子集DRG亚组编码生成完毕")
  }



}
