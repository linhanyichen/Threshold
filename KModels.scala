package org.deep.threshold.proc

import java.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util._

/**
  * K-models 聚类算法
  * create by zhuy 2019-10-28
  */
class KModels(spark:SparkSession) {

  private [this] var df:DataFrame = null
  private [this] var subGroupFeeDF:DataFrame = null                            // 子集费用分析DF
  private [this] var centers:Array[Vector[Int]] = new Array[Vector[Int]](3)    // 中心点,默认为3个
  private [this] var samples:Array[Vector[Int]] = null                         // 样本数据
  private [this] final val MAX_LOOP_COUNT:Int = 10
  private [this] var bContinue:Boolean = true
  private [this] val sampleMap:util.HashMap[Int,String] = new util.HashMap[Int,String]()

  private [this] var mainCode:String = ""
  private [this] var subCode:String = ""
  private [this] var subFeeDataFilePath:String = ""

  // Kmodels 生成的子集码列表
  private [this] var subCodeList:List[String] = List.empty

  // 需要再进行Kmodels聚类处理的子集列表
  private [this] var reCalcSubCOdeList:List[String] = List.empty

  // 子集数据是否需要从子集费用表中删除
  private [this] var delFromSubFee:Boolean = false


  def runKModels(sDF:DataFrame,mCode:String,sCode:String) :Unit = {
    df = sDF
    mainCode = mCode
    subCode = sCode
    subFeeDataFilePath = Dict.subFeeDataPath.replace("&trunkcode", mainCode)
    subGroupFeeDF = spark.read.parquet(subFeeDataFilePath)

    LogUtil.logINFO(s"开始对[主干组 -> ${mainCode}][子集码 -> ${subCode}]数据按Kmodels聚类进行子集划分")

    // 1.生成向量数据集
    samples = genSamples

    // 2.生成聚类中心点
    initCenters

    // 3.对样本数据进行聚类处理
    kmeans

    // 4.根据聚类结果更新子集码
    updSubCode
  }


  /**
    * 获取子集码列表
    * @return
    */
  def getsubCodeList() :List[String] = {
    subCodeList
  }

  /**
    * 子集数据是否需要从子集费用表中删除
    * @return
    */
  def bDelFromSubFee() :Boolean = {
    delFromSubFee
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
    val sampleDF = df.select("`病历序号`","a1","a2","d1","d2","g","r","s","u")
    var fieldMap:Map[String,String]  = Map.empty
    sampleDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    var iLoop:Int = 0
    val cols:util.LinkedList[util.HashMap[String, Object]] = DFUtil.DF2List(sampleDF,fieldMap)
    val sampleArr:Array[Vector[Int]] = cols.toArray.map(col => {
      val id:String = col.asInstanceOf[util.HashMap[String, Object]].get("病历序号").toString
      val a1:Int = col.asInstanceOf[util.HashMap[String, Object]].get("a1").toString.toInt
      val a2:Int = col.asInstanceOf[util.HashMap[String, Object]].get("a2").toString.toInt
      val d1:Int = col.asInstanceOf[util.HashMap[String, Object]].get("d1").toString.toInt
      val d2:Int = col.asInstanceOf[util.HashMap[String, Object]].get("d2").toString.toInt
      val g:Int = col.asInstanceOf[util.HashMap[String, Object]].get("g").toString.toInt
      val r:Int = col.asInstanceOf[util.HashMap[String, Object]].get("r").toString.toInt
      val s:Int = col.asInstanceOf[util.HashMap[String, Object]].get("s").toString.toInt
      val u:Int = col.asInstanceOf[util.HashMap[String, Object]].get("u").toString.toInt
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
    // 大于两份样本数据则随机选取向量中心点
//    LogUtil.logINFO(s"对样本数据进行随机向量中心点选取处理,样本病历份数为[${samples.length}]")

    val groupMap = samples.groupBy(v => v)
    val keyCnt = groupMap.keySet.size

    // 所有属性向量相同则不再做处理
    if (keyCnt == 1) {
      LogUtil.logDEBUG(s"所有样本病历属性向量均相同,不再进行Kmodels聚类处理")
      subCodeList = List(subCode)
      bContinue = false
      return
    }

    if (keyCnt == 2) {
      centers = centers.dropRight(1)
    }

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
        LogUtil.logINFO(s"新旧中心点距离为0,不再进行聚类处理,迭代次数为[$loopCnt]")
        run = false
      }

      if (loopCnt > MAX_LOOP_COUNT){
        LogUtil.logINFO("Kmodels聚类处理次数超过10次,不再进行聚类处理")
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
    val filterList:util.List[String] = new util.LinkedList[String]()
    for (i<-0 to centers.length-1) filterList.add("`病历序号` in (")

    var closetCenter:Vector[Int] = Vector[Int]()
    for(i<-0 to sampleNum -1){
      closetCenter = centers.reduceLeft((c1,c2) => if(vectorDis(c1,samples(i)) < vectorDis(c2,samples(i))) c1 else c2 )

      val index = centers.indexOf(closetCenter)
      filterList.set(index, filterList.get(index) + s"'${sampleMap.get(i.toInt).toString}' ,")
    }

    for (i<-0 to centers.length-1){
      // 聚类中心点对应的样本个数不为0时才进行子集划分
      if(!filterList.get(i).equals("`病历序号` in (")){
        val whereSQL:String = filterList.get(i).substring(0,filterList.get(i).length-1) + ")"
        var subDF:DataFrame = df.where(whereSQL)

        subDF = subDF.drop("子集码")
        subDF = subDF.withColumn("子集码", functions.lit(subCodeList(i)))

        // 更新子集码
        appendSubFeeData(subDF,subCodeList(i))

        val path = Dict.setDataPath.replaceAll("&maincode", mainCode)+s"${subCodeList(i)}.parquet"
        subDF.write.mode(SaveMode.Overwrite).parquet(path)
      }
    }

    // 删除原子集数据
    HDFSUtil.deleteFile(Dict.setDataPath.replaceAll("&maincode", mainCode)+s"${subCode}.parquet")
    delFromSubFee = true
  }


  /**
    * 追加子集数据
    * @param dataDF  待计算的原始数据集
    * @param subCode 子集码
    */
  private def appendSubFeeData(dataDF:DataFrame,subCode:String) :Unit = {

    val dfTmp = dataDF.selectExpr("avg(`医疗费用`) as `费用均值`","percentile(`医疗费用`,0.5) as `中位数`",
      "stddev_pop(`医疗费用`) as `标准差`","stddev_pop(`医疗费用`)/avg(`医疗费用`) as `变异系数`",
      "max(`医疗费用`) as `最高费用`","min(`医疗费用`) as `最低费用`","count(`病历序号`) as `病历份数`",
      s"'${subCode}' as `子集码`", "'' as `DRG组编码`").toDF()

    val list = getDF2List(dfTmp)
    val vc:Double = list.get(0).get("变异系数").toString.toDouble
    val cnt:Int = list.get(0).get("病历份数").toString.toInt

    // 变异系数小于0.5 或者 变异系数>=0.5但病历份数小于10份的不再进行Kmodels聚类
    // 否则取变异系数大于等于0.5的继续进行Kmodels聚类处理
    if (!(vc < 0.5 ||(vc >= 0.5 && cnt < 10))){
      if (vc >= 0.5 && cnt > 10)  reCalcSubCOdeList ++=  List(subCode)
    }

    dfTmp.write.mode(SaveMode.Append).parquet(subFeeDataFilePath)
  }

  /**
    * 将DF转成List[Map]
    * @param sDF
    * @return
    */
  protected def getDF2List(sDF:DataFrame) :util.List[util.HashMap[String,Object]] = {
    var fieldMap:Map[String,String]  = Map.empty
    sDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    DFUtil.DF2List(sDF,fieldMap)
  }

}
