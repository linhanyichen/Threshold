package org.deep.threshold.memproc

import java.util

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{LogUtil, StatUtil}

/**
  * 子集聚类划分亚组(内存版)
  * create by zhuy 2019-11-20
  */
class SSKmeansProc(spark:SparkSession) extends ProcTrait {
  // 有效的主干组数据
  private [this] var remainDataMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()
  // 主干组数据内存存储Map
  private [this] var dataMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()
  // 子集费用分析表
  private [this] var subFeeMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()

  private [this] var groupNum:Int = 0 // 亚组个数
  private [this] val medianData:util.HashMap[String,Double] = new util.HashMap[String,Double]()
  private [this] var drgsCodeMap:util.HashMap[String,String] = new util.HashMap[String,String]()  // 子集亚组编码关系map,key-子集码,value-亚组编码
  private [this] val kmeans:MedianKmeans = new MedianKmeans
  private [this] var delCodeList:List[String] = List.empty

  //add by zhuy 2020-01-21 for 子集划分变异系数值参数化处理
  private [this] var cvList:List[Double] = List(0.4,0.6,0.8)

  var schema:StructType = null
  var feeSchema:StructType = null
  var subSetNumber:Int = 0   // 子集个数

  /**
    * 初始化亚组分组变异系数参数值
    * @param list
    */
  def initCVs(list:List[Double]) :Unit = {
    cvList = List.empty
    list.foreach(v => cvList ++= List(v))
  }

  /**
    * 初始化数据结构信息
    * @param dataSchema
    * @param feeDataSchema
    */
  def initSchema(dataSchema:StructType,feeDataSchema:StructType) :Unit = {
    schema = dataSchema
    feeSchema = feeDataSchema
  }

  def runSSKmeansProc(dMap:util.HashMap[String,util.HashMap[String,Object]],sFeeMap:util.HashMap[String,util.HashMap[String,Object]],mCode:String) :Unit = {
    dataMap = dMap
    subFeeMap = sFeeMap
    mainCode = mCode
    subFeeDataFilePath = Dict.subFeeDataPath.replace("&maincode",mainCode)

    // 1.过滤无效子集
    filterSubSet()

    // 2.计算所有病历的变异系数
    calcAllDataCV()

    // 3.中位数Kmeans聚类
    if(groupNum > 1) kmeans.runMedianKmeans(mainCode,groupNum,medianData)

    // 4.更新病历所属亚组信息
    upgGroupInfo

    // 5.写子集,全集以及子集费用分析数据
    saveData2HDFS

  }

  /**
    * 获取亚组个数
    * @return
    */
  def getGroupNumber() :Int = {
    groupNum
  }

  /**
    * 获取亚组与子集关系Map
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
    LogUtil.logINFO(s"[主干组 -> ${mainCode}] 集数共[${subFeeMap.keySet().size}]个")

    subFeeMap.keySet().toArray.foreach(code => {
      val setCode = code.toString

      val cv:Double = subFeeMap.get(setCode).get("变异系数").toString.toDouble
      val orderCnt:Long = subFeeMap.get(setCode).get("病历份数").toString.toLong

      if (orderCnt <= 2) delCodeList ++= List(setCode)
      else if (orderCnt >=3 &&  orderCnt <=5 && cv >0.5) delCodeList ++= List(setCode)
      else if (orderCnt >=6 &&  orderCnt <=9 && cv >0.6) delCodeList ++= List(setCode)
      else if (orderCnt >=10 && cv >= 1) delCodeList ++= List(setCode)
    })

    LogUtil.logINFO(s"[主干组 -> ${mainCode}] 无效子集数共[${delCodeList.size}]个,全部删除")
    delCodeList.foreach(code => subFeeMap.remove(code))
  }

  /**
    * 计算所有病历费用的变异系数值
    */
  def calcAllDataCV() :Unit = {

    var feeList:List[Double] = List.empty

    dataMap.keySet().toArray.foreach(id => {
      val orderId :String = id.toString
      val set_code:String = dataMap.get(orderId).get("set_code").toString

      // 有效病历数据
      if (delCodeList.indexOf(set_code) < 0){
        remainDataMap.put(orderId,dataMap.get(orderId))
        feeList ++= List(dataMap.get(orderId).get("fee").toString.toDouble)
      }
    })

    val cvVaule:Double = StatUtil.cv(feeList)
    LogUtil.logINFO(s"样本数据变异系数为[${cvVaule}],亚组细分参数为:${cvList(0)}、${cvList(1)}、${cvList(2)}")

    if (cvVaule <= cvList(0))    groupNum = 1
    else if (cvVaule > cvList(0) && cvVaule <= cvList(1)) groupNum = 2
    else if (cvVaule > cvList(1) && cvVaule <= cvList(2)) groupNum = 3
    else groupNum = 4

    LogUtil.logINFO(s"[主干组 -> ${mainCode}] 有效数据计算变异系数,并确定亚组个数为[${groupNum}]个")

    // 取各子集码对应中位数信息
    subFeeMap.keySet().toArray.foreach(code => {
      if (groupNum == 1) drgsCodeMap.put(code.toString,s"${mainCode}9")   // 只有一个亚组,直接补9
      medianData.put(code.toString, subFeeMap.get(code.toString).get("中位数").toString.toDouble)
    })

  }

  /**
    * 更新病历所属亚组信息
    */
  private def upgGroupInfo() :Unit = {
    if(groupNum > 1) drgsCodeMap = kmeans.getDrgsCodeMap()

    remainDataMap.keySet().toArray.foreach(id => {
      val orderId :String = id.toString
      val set_code:String = remainDataMap.get(orderId).get("set_code").toString

      // 只有一个亚组,则补9
      if(groupNum == 1){
        remainDataMap.get(orderId).put("drg-group", s"${mainCode}9")
      }else{
        if (drgsCodeMap.containsKey(set_code)) remainDataMap.get(orderId).put("drg-group", drgsCodeMap.get(set_code))
        else  remainDataMap.get(orderId).put("drg-group", "")
      }
    })
  }


  /**
    * 保存最终数据到HDFS
    */
  private def saveData2HDFS() :Unit = {
    var feeSeq:Seq[Row] = Seq.empty
    var allDataSeq:Seq[Row] = Seq.empty

    var scList:List[StructField] = List.empty
    // 生成新的数据结构
    schema.fieldNames.foreach(field => scList ++= List(schema(field)) )
    scList ++= List(StructField("子集码", StringType, nullable = false))

    val allDataSchema:StructType = StructType(scList)

    val total = subFeeMap.keySet().size
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]共[${remainDataMap.keySet().size()}]条有效数据")

    var setCnt:Int = 0
    subFeeMap.keySet().toArray.foreach(key => {
      val code = key.toString
      var dataSeq:Seq[Row] = Seq.empty
      remainDataMap.keySet().toArray.foreach(k => {
        if (remainDataMap.get(k.toString).get("set_code").toString.equals(code)){
          val dRow = remainDataMap.get(k.toString).get("row").asInstanceOf[Row]
          dataSeq ++= Seq(dRow)

          // 组装全集数据的Row
          var data:Seq[Any] = Seq.empty
          allDataSchema.fieldNames.foreach(field => {
            field match {
              case "子集码" => data ++= Seq(code)
              case _ =>{
                allDataSchema(field).dataType.typeName match {
                  case "string" => data ++= Seq(dRow.getAs(field).toString)
                  case "double" => data ++= Seq(dRow.getAs(field).toString.toDouble)
                  case "long" => data ++= Seq(dRow.getAs(field).toString.toLong)
                  case "integer" => data ++= Seq(dRow.getAs(field).toString.toInt)
                  case "decimal" => data ++= Seq(dRow.getAs(field).toString.toDouble)
                  case _ => data ++= Seq(dRow.getAs(field).toString)
                }
              }
            }
          })

          allDataSeq ++= Seq(Row.fromSeq(data))
        }
      })

      if (!dataSeq.isEmpty){
        val rdd = spark.sparkContext.parallelize(dataSeq)
        val fee:Seq[Any] = Seq(subFeeMap.get(code).get("费用均值").toString.toDouble,
          subFeeMap.get(code).get("中位数").toString.toDouble,subFeeMap.get(code).get("标准差").toString.toDouble,
          subFeeMap.get(code).get("变异系数").toString.toDouble,subFeeMap.get(code).get("最高费用").toString.toDouble,
          subFeeMap.get(code).get("最低费用").toString.toDouble,subFeeMap.get(code).get("病历份数").toString.toDouble.toLong,
          code,drgsCodeMap.get(code))

        feeSeq ++= Seq(Row.fromSeq(fee))
        var dfTmp:DataFrame = spark.sqlContext.createDataFrame(rdd, schema)
        dfTmp = dfTmp.withColumn("子集码", functions.lit(code))
        val path:String = Dict.setDataPath.replaceAll("&maincode", mainCode)+ code+".parquet"
        dfTmp.repartition(5).write.mode(SaveMode.Overwrite).parquet(path)
        setCnt = setCnt + 1

        if ((setCnt % math.floor(total/10.0) == 0))  LogUtil.logINFO(s"[主干组 -> ${mainCode}]已完成[${(setCnt*100.0/total).formatted("%.2f").toString}%]的子集磁盘写入处理")
      }
    })

    val feeRdd = spark.sparkContext.parallelize(feeSeq)
    val feeDF = spark.sqlContext.createDataFrame(feeRdd, feeSchema)
    feeDF.write.mode(SaveMode.Overwrite).parquet(subFeeDataFilePath)


    // 全集数据单独写文件
    val rdd = spark.sparkContext.parallelize(allDataSeq)
    val dfTmp:DataFrame = spark.sqlContext.createDataFrame(rdd, allDataSchema)
    val path:String = Dict.setDataPath.replaceAll("&maincode", mainCode)+s"${mainCode}_all.parquet"
    dfTmp.write.mode(SaveMode.Overwrite).parquet(path)

    subSetNumber = subFeeMap.keySet().size()
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]有效病历份数为[${remainDataMap.keySet().size()}],子集个数为[${subSetNumber}],亚组个数为[${groupNum}]")
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]亚组划分处理完毕")
  }

}
