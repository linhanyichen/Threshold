package org.deep.threshold.memproc

import java.util

import org.apache.spark.sql.Row
import org.deep.threshold.dict.Dict
import org.deep.threshold.util._

/**
  * K-modes 聚类算法(内存版)
  * create by zhuy 2019-11-20
  */
class KModes() {   // 子集费用分析DF
  private [this] var centers:Array[Vector[Int]] = new Array[Vector[Int]](3)    // 中心点,默认为3个
  private [this] var samples:Array[Vector[Int]] = null                         // 样本数据
  private [this] final val MAX_LOOP_COUNT:Int = 10
  private [this] var bContinue:Boolean = true
  private [this] val sampleMap:util.HashMap[Int,String] = new util.HashMap[Int,String]()

  private [this] var mainCode:String = ""
  private [this] var subCode:String = ""
  private [this] var subFeeDataFilePath:String = ""

  // 主干组数据内存存储Map
  private [this] var dataMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()
  // 子集费用分析表
  private [this] var subFeeMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()

  // Kmodels 生成的子集码列表
  private [this] var subCodeList:List[String] = List.empty
  // 需要再进行Kmodels聚类处理的子集列表
  private [this] var reCalcSubCOdeList:List[String] = List.empty


  def runKModels(setData:util.HashMap[String,util.HashMap[String,Object]],
                 feeData:util.HashMap[String,util.HashMap[String,Object]],
                 mCode:String,sCode:String) :Unit = {

    dataMap = setData
    subFeeMap = feeData
    mainCode = mCode
    subCode = sCode
    subFeeDataFilePath = Dict.subFeeDataPath.replace("&trunkcode", mainCode)

    // 1.生成向量数据集
    samples = genSamples

    // 2.生成聚类中心点
    initCenters

    // 3.对样本数据进行聚类处理
    kmeans

    // 4.根据聚类结果更新子集码
    updSubCode
  }

  // 获取更新子集码后的病历数据信息
  def getMapData() :util.HashMap[String,util.HashMap[String,Object]] = {
    dataMap
  }

  /**
    * 获取需再次计算的子集码列表
    * @return
    */
  def getreCalcSubCOdeList() :List[String] = {
    reCalcSubCOdeList
  }

  /**
    * 获取子集属性向量并生成样本数据
    * @return
    */
  private def genSamples() :Array[Vector[Int]] = {
    var iLoop:Int = 0
    val sampleArr:Array[Vector[Int]] = dataMap.keySet().toArray.map(order_id => {
      val id:String = order_id.toString
      val row:Row = dataMap.get(id).get("row").asInstanceOf[Row]

      val a1:Int = row.getAs("a1").toString.toInt
      val a2:Int = row.getAs("a2").toString.toInt
      val d1:Int = row.getAs("d1").toString.toInt
      val d2:Int = row.getAs("d1").toString.toInt
      val g:Int = row.getAs("g").toString.toInt
      val r:Int = row.getAs("r").toString.toInt
      val s:Int = row.getAs("s").toString.toInt
      val u:Int = row.getAs("u").toString.toInt
      val vec:Vector[Int] = Vector(a1,a2,d1,d2,g,r,s,u)

      sampleMap.put(iLoop,id)
      iLoop= iLoop + 1
      vec
    })

    sampleArr
  }

  /**
    * 初始化中心点
    */
  private def initCenters() :Unit = {
    val groupMap = samples.groupBy(v => v)
    val keyCnt = groupMap.keySet.size

    // 所有属性向量相同则不再做处理
    if (keyCnt == 1) {
      subCodeList = List(subCode)
      bContinue = false
      return
    }

    if (keyCnt == 2) centers = centers.dropRight(1)
    for(i<-0 to centers.size-1) centers.update(i, groupMap.keySet.toList(i))
  }

  /**
    * KMeans迭代聚类
    */
  private def kmeans() :Unit = {
    if (!bContinue) return

    var run:Boolean = true
    var loopCnt:Int = 1
    var newCenters = Array[Vector[Int]]()

    while(run){

      // 按中心点进行聚类
      val cluster = samples.groupBy(v => closesetCenter(v))
      newCenters = centers.map(oldCenter => {
        cluster.get(oldCenter) match {//找到该聚类中心所拥有的点集
          case Some(pointsInThisCluster) => genNewCenter(pointsInThisCluster)
          case None => oldCenter
        }
      })

      /**
        * 聚类次数小于10次或者新旧中心点不重合则继续进行聚类计算
        */
      if (isNewCenterLegal(newCenters)){
        run = false
      }

      if (loopCnt > MAX_LOOP_COUNT){
        run = false
      }

      // 更新中心点
      for (i<-0 to centers.length-1){
        centers.update(i, newCenters(i))
        subCodeList ++= List(SubCodeUtil.getSubCode()+"-2KM")
      }

      loopCnt = loopCnt + 1
    }
  }


  /**
    * 计算样本点所属的聚类中心
    * @param v 样本点
    * @return 聚类中心点
    */
  private def closesetCenter(v:Vector[Int]) :Vector[Int] = {
    centers.reduceLeft((c1,c2) => {
      if(vectorDis(c1,v) < vectorDis(c2,v)) c1 else c2
    })
  }

  /**
    * 属性向量距离计算
    * dis = (M01+M10)/(M00+M01+M10+M11)
    * M00代表向量A与向量B中值均为0的维度个数
    * M01代表向量A为0,而向量B中值为1的维度个数
    * M10代表向量A为1,而向量B中值为0的维度个数
    * M11代表向量A与向量B中值均为1的维度个数
    * @param A 中心的属性向量
    * @param B 样本点属性向量
    * @return 属性向量距离
    */
  private def vectorDis(A:Vector[Int], B:Vector[Int]) :Double = {
    var M00:Int = 0
    var M01:Int = 0
    var M10:Int = 0
    var M11:Int = 0

    for (i<-0 to A.size-1){
      if (A(i) ==0 && B(i) ==0)      M00 = M00 + 1
      else if (A(i) ==0 && B(i) ==1) M01 = M01 + 1
      else if (A(i) ==1 && B(i) ==0) M10 = M10 + 1
      else                           M11 = M11 + 1
    }

    (M01+M10)*1.0/(M00+M01+M10+M11)
  }


  /**
    * 根据聚类结果计算新的中心点
    * @param sample
    * @return
    */
  private def genNewCenter(sample:Array[Vector[Int]]) :Vector[Int] ={
    var newCenter:Vector[Int] = Vector(0,0,0,0,0,0,0,0)

    for (i<-0 to sample.length -1){
      for (j<-0 to newCenter.length -1){
        /**
          * 新中心点的各属性默认为0,若样本点属性为0则减1,否则加1
          * 最后根据新中心点的属性值做更新:
          * (1)若中心点属性值大于0说明1较多,中心点属性更新为1
          * (2)若中心点属性值等于0说明0和1同样多,中心点属性更新为0
          * (3)若中心点属性值小于0说明0较多,中心点属性更新为0
          */
        val zNumber :Int = if (sample(i)(j) == 0)  newCenter(j) -1 else newCenter(j) + 1
        newCenter = newCenter.updated(j,zNumber)
      }
    }

    for (j<-0 to newCenter.length -1){
      var  zNumber:Int = 0

      /**
        * 众数为0则取值为0,众数为1则取值为1,若01个数相等则随机取0或1
        */
      if (newCenter(j) > 0)      zNumber = 1
      else if (newCenter(j) < 0) zNumber =0
      else                       zNumber = RandomUtil.getIntRandom(100000)%8999%2

      newCenter = newCenter.updated(j,zNumber)
    }

    newCenter
  }

  /**
    * 新聚类中心点是否合法
    * @param newCenters 新中心点
    * @return true-新中心点与旧中心点距离为0,false-新中心点与旧中心点距离不为0
    */
  private def isNewCenterLegal(newCenters:Array[Vector[Int]]) :Boolean = {
    for (i<-0 to newCenters.length-1){
      if(vectorDis(newCenters(i),centers(i)) > 0.0) return false
    }

    true
  }

  /**
    * 根据聚类结果更新子集码
    */
  private def updSubCode() :Unit = {
    if (!bContinue) return

    val sampleNum :Int = samples.length
    var feeLists:List[List[Double]] = List.empty
    var orderCntLists:List[Long] = List.empty
    val indexMap:util.HashMap[String,Int] = new util.HashMap[String,Int]()

    for (i<-0 to subCodeList.length -1){
      feeLists ++= List(List.empty)
      orderCntLists ++= List(0.toLong)
      indexMap.put(subCodeList(i),i)
    }

    var closetCenter:Vector[Int] = Vector[Int]()
    for(i<-0 to sampleNum -1){
      // 计算样本所属中心点
      closetCenter = centers.reduceLeft((c1,c2) => if(vectorDis(c1,samples(i)) < vectorDis(c2,samples(i))) c1 else c2 )

      val cindex = centers.indexOf(closetCenter)
      val orderId:String = sampleMap.get(i).toString
      val set_code:String = subCodeList(cindex)

      // 获取子集所属List下标
      val index:Int = indexMap.get(set_code)
      feeLists = feeLists.updated(index, feeLists(index) ++ List(dataMap.get(orderId).get("fee").toString.toDouble))
      orderCntLists = orderCntLists.updated(index, orderCntLists(index) + 1.toLong)

      // 更新病历所属子集
      dataMap.get(orderId).put("set_code",set_code)
    }

    // 计算新的子集费用数据信息
    for (i<-0 to subCodeList.length -1){
      val set_code = subCodeList(i)
      val feeList = feeLists(indexMap.get(set_code))       // 费用列表
      val orderCnt = orderCntLists(indexMap.get(set_code)) // 病历份数

      // 聚类点有对应样本数据时才进行子集费用处理
      if(feeList.length > 0){
        val propMap:util.HashMap[String,Object] = new util.HashMap[String,Object]()

        val cv:Double = StatUtil.cv(feeList)
        propMap.put("费用均值", StatUtil.avg(feeList).asInstanceOf[Object])
        propMap.put("中位数", StatUtil.median(feeList).asInstanceOf[Object])
        propMap.put("标准差", StatUtil.stdev(feeList).asInstanceOf[Object])
        propMap.put("变异系数", cv.asInstanceOf[Object])
        propMap.put("最高费用", StatUtil.max(feeList).asInstanceOf[Object])
        propMap.put("最低费用", StatUtil.min(feeList).asInstanceOf[Object])
        propMap.put("病历份数", orderCnt.asInstanceOf[Object])
        subFeeMap.put(set_code,propMap)

        // 变异系数小于0.5 或者 变异系数>=0.5但病历份数小于10份的不再进行Kmodels聚类
        // 否则取变异系数大于等于0.5的继续进行Kmodels聚类处理
        if (!(cv < 0.5 ||(cv >= 0.5 && orderCnt < 10))){
          if (cv >= 0.5 && orderCnt > 10)  reCalcSubCOdeList ++=  List(set_code)
        }
      }
    }

    // 删除原子集信息
    subFeeMap.remove(subCode)
  }

}
