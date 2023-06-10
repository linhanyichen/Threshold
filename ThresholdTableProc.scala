package org.deep.threshold.proc

import java.util

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{DFExcelUtil, HDFSUtil, LogUtil, ShellUtil}

import scala.actors.threadpool.ExecutorService

/**
  * 阈值表生成处理类
  * 其中 TF(m,n) = 子集m中属性n的值/子集m中属性累加值
  *      IDF(n) = log10(所有子集个数/包含属性n的子集个数)
  *      权系数(m,n) = TF(m,n) * IDF(n)
  */
class ThresholdTableProc(spark:SparkSession) extends ProcTrait {
  private [this] var tTableProp:List[String] = List.empty        // 阈值表属性字段列表
  private [this] var staticTTProp:List[String] = List.empty      // 阈值表静态属性字段列表
  private [this] var bContinue:Boolean = true
  private [this] var subCodeList:List[String] = List.empty       // 子集码列表
  private [this] var schemaList:List[StructField] = List.empty   // 阈值表字段结构列表
  private [this] var schema:StructType = null                    // 阈值表表结构
  private [this] var TTFilePath:String = ""                      // 阈值表文件路径
  private [this] var subFeeDataPath:String = ""
  // 各动态属性的IDF值
  private [this] val IDFMap:util.HashMap[String,Double] = new util.HashMap[String,Double]()
  private [this] val weightMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()

  // 子集码与DRG组编码关系Map
  private [this] var drgsCodeMap:util.HashMap[String,String] = new util.HashMap[String,String]()
  private var executorService: ExecutorService = null

  def initExecutorService(es:ExecutorService) :Unit = {
    executorService = es
  }

  /**
    * 阈值表生成处理
    * @param mCode    主干组编码
    * @param codeMap  子集码和亚组编码对应关系
    */
  def runTTProc(mCode:String,codeMap:util.HashMap[String,String]) :Unit = {
    mainCode = mCode
    drgsCodeMap = codeMap
    dataSavePath = Dict.setDataPath.replace("&maincode",mainCode)
    TTFilePath = s"${dataSavePath}ThresholdTable.parquet"
    subFeeDataPath = Dict.subFeeDataPath.replace("&maincode",mainCode)


    // 1.初始化阈值表结构信息
    init

    // 2.获取动态属性
    getDynamicProp

    // 3.对子集中各属性进行累加处理
    calcWeigths

    // 4.生成阈值表
    genThresholdTable

    // 5.将阈值表数据写入Excel
    writeExcel

  }

  private def init() :Unit = {
    schemaList ++= List(StructField("DRG", StringType, nullable = false))

    df = spark.read.parquet(s"${dataSavePath}${mainCode}_all.parquet")
    val subFeeDF = spark.read.parquet(subFeeDataPath)
    getDFValueList(subFeeDF.select("`子集码`"),"子集码").toArray.foreach(code => subCodeList ++= List(code.toString))
  }


  /**
    * 获取动态属性列表
    */
  private def getDynamicProp() :Unit = {
    if (!bContinue) return

    val sList:List[String] = List("NL.01","NL.02","NL.03","RY.01","RY.02","RY.03","RY.04","QJ","BW","ICU","DJ")
    var retList:util.List[String] = new util.LinkedList[String]()
    val subSetCnt = subCodeList.size
    var IDF:Double = 0.00

    var lFlag:Int = 0
    // 固定向量的IDF值计算
    sList.foreach(prop => {
        if(prop.equals("BW")) retList = getDFValueList(df.filter(s"SW = '1' ").selectExpr("count (distinct `子集码`) as `子集数`"),"子集数")
        else retList = getDFValueList(df.filter(s"`${prop}` = '1' ").selectExpr("count (distinct `子集码`) as `子集数`"),"子集数")

        // 如果属性未出现在任何子集中,则该属性无需写入阈值表
        if (!retList.get(0).toString.equals("0")){
          IDF = math.log10(subSetCnt/retList.get(0).toDouble)
          LogUtil.logDEBUG(s"属性[${prop}] IDF值为 [${IDF}]")

          tTableProp ++= List(prop)
          staticTTProp ++= List(prop)
          schemaList ++= List(StructField(s"${prop}", DoubleType, nullable = false))
          IDFMap.put(prop,IDF)
        }
    })


    // 将DRG编码、子集码 属性加入tTableProp
    tTableProp = "DRG"::tTableProp
    df = spark.read.parquet(s"${dataSavePath}${mainCode}_all.parquet")

    retList = getDFValueList(df.groupBy("`主诊断`").count(),"主诊断")
    retList.toArray.foreach(value => {
      if(!value.toString.equals("")){
        tTableProp ++= List(value.toString)
        schemaList ++= List(StructField(s"${value.toString}", DoubleType, nullable = false))

        val rList = getDFValueList(df.filter(s"`主诊断` = '${value.toString}' ").selectExpr("count (distinct `子集码`) as `子集数`"),"子集数")
        if (rList.get(0).toString.equals("0")) IDF = 0
        else IDF = math.log10(subSetCnt/rList.get(0).toDouble)

        if ((lFlag % math.floor(retList.size/10.0) == 0)) LogUtil.logDEBUG(s"第[${lFlag}]个主诊断属性计算完毕,当前为该阶段[${(lFlag+1)*100/retList.size}%]的计算处理")
        IDFMap.put(value.toString,IDF)
      }

      lFlag = lFlag +1
    })

    lFlag = 0
    retList = getDFValueList(df.groupBy("`辅诊断`").count(),"辅诊断")
    retList.toArray.foreach(value => {
      if(!value.toString.equals("")) {
        tTableProp ++= List(value.toString)

        schemaList ++= List(StructField(s"${value.toString}", DoubleType, nullable = false))

        val rList = getDFValueList(df.filter(s"`辅诊断` = '${value.toString}' ").selectExpr("count (distinct `子集码`) as `子集数`"),"子集数")

        if (rList.get(0).toString.equals("0")) IDF = 0
        else IDF = math.log10(subSetCnt/rList.get(0).toDouble)
        if ((lFlag % math.floor(retList.size/10.0) == 0)) LogUtil.logDEBUG(s"第[${lFlag}]个辅诊断属性计算完毕,当前为该阶段[${(lFlag+1)*100/retList.size}%]的计算处理")
        IDFMap.put(value.toString,IDF)
      }

      lFlag = lFlag +1
    })


    lFlag = 0
    retList = getDFValueList(df.groupBy("`手术`").count(),"手术")
    retList.toArray.foreach(value => {
      if (!value.toString.equals("")) {
        tTableProp ++= List(value.toString)
        schemaList ++= List(StructField(s"${value.toString}", DoubleType, nullable = false))

        val rList = getDFValueList(df.filter(s"`手术` = '${value.toString}' ").selectExpr("count (distinct `子集码`) as `子集数`"), "子集数")
        if (rList.get(0).toString.equals("0")) IDF = 0
        else IDF = math.log10(subSetCnt / rList.get(0).toDouble)
        if ((lFlag % math.floor(retList.size / 10.0) == 0)) LogUtil.logDEBUG(s"第[${lFlag}]个手术属性计算完毕,当前为该阶段[${(lFlag + 1) * 100 / retList.size}%]的计算处理")
        IDFMap.put(value.toString, IDF)
      }

      lFlag = lFlag + 1
    })

    // 生成阈值表表结构schema
    schema = StructType(schemaList)
  }


  /**
    * 计算权系数
    */
  private def calcWeigths() :Unit = {
    if (!bContinue) return

    var lFlag:Int = 0
    subCodeList.foreach(code => {
      var bInit:Boolean = false
      var total:Double = 0.00

      val setDF = spark.read.parquet(s"${dataSavePath}${code}.parquet")
      val TFMap :util.HashMap[String,Object] = initTFMap()

      getDF2List(setDF).toArray.foreach(row => {
        val rMap = row.asInstanceOf[util.HashMap[String, Object]]

        if (!bInit){
          TFMap.put("DRG",drgsCodeMap.get(code).toString)
          bInit = true
        }

        rMap.keySet().toArray.foreach(key => {
          val colName = key.toString
          val colValue = rMap.get(colName).toString

          // 更新动态属性
          if (colName.equals("主诊断") || colName.equals("手术") || colName.equals("辅诊断")){
            tTableProp.indexOf(colValue) match {
              case -1 => None
              case _ => { TFMap.put(colValue, (TFMap.get(colValue).asInstanceOf[Double] + 1).asInstanceOf[Object]) ;total = total + 1}
            }
          }

          // 更新静态属性
          val cName :String = if(colName.equals("SW")) "BW" else colName
          if(staticTTProp.indexOf(colName) > -1){
            colValue match {
              case "1" => { TFMap.put(colName, (TFMap.get(colName).asInstanceOf[Double] + 1).asInstanceOf[Object]) ; total = total + 1 }
              case _ => None
            }
          }
        })
      })

      // 计算TF值,保留9位有效数字
      tTableProp.foreach(prop => {if (!prop.equals("DRG")) TFMap.put(prop, (TFMap.get(prop).asInstanceOf[Double] / total).formatted("%.9f").toDouble.asInstanceOf[Object]) })

      // 计算TF(m,n)*IDF(n)
      tTableProp.foreach(prop => {
        if (!prop.equals("DRG")){
          val tf = IDFMap.get(prop).toString.toDouble
          val idf = TFMap.get(prop).toString.toDouble
          TFMap.put(prop,(tf*idf).asInstanceOf[Object])
        }
      })

      // 计算TF值并写入权系数Map
      weightMap.put(code,TFMap)
      lFlag = lFlag + 1
      if ((lFlag % math.floor(subCodeList.size/10.0) == 0)) LogUtil.logINFO(s"[主干组 -> ${mainCode}]${code}子集计算完毕,已完成该阶段[${lFlag*100/subCodeList.size}%]的权系数计算处理")
    })

    while (lFlag < subCodeList.size) {Thread.sleep(10)}
  }


/**
  * 初始化TF
    * @return
    */
  private def initTFMap() :util.HashMap[String,Object] = {
    val ret:util.HashMap[String,Object] = new util.HashMap[String,Object]()
    ret.put("DRG","")

    tTableProp.foreach(prop => if(!prop.equals("DRG")) ret.put(prop,0.00.asInstanceOf[Object]))
    ret
  }

  /**
    * 生成阈值表
    */
  private def genThresholdTable() :Unit = {
    val saveMode:SaveMode = if (HDFSUtil.isParquetFileExist(TTFilePath)) SaveMode.Append else SaveMode.Overwrite

    var data:Seq[Any] = Seq.empty
    var rowSeq:Seq[Row] = Seq.empty

    weightMap.keySet().toArray.foreach(code => {

      data = Seq.empty
      val rowMap:util.HashMap[String,Object] = weightMap.get(code.toString)

      tTableProp.foreach(colName => {
        colName match  {
          case "DRG" => data ++= Seq(rowMap.get(colName).toString)
          case _ => data ++= Seq(rowMap.get(colName).asInstanceOf[Double])
        }
      })// end of foreach

      rowSeq ++= Seq(Row.fromSeq(data))
    })

    val rdd = spark.sparkContext.parallelize(rowSeq)
    df = spark.sqlContext.createDataFrame(rdd, schema)
    df.write.mode(saveMode).parquet(TTFilePath)

    LogUtil.logINFO(s"-----------------[主干组 -> ${mainCode}]阈值表数据处理完毕,写入路径[${TTFilePath}]")
  }


  /**
    * 将阈值表数据写入Excel
    */
  private def writeExcel() :Unit = {
    val dfExcel = spark.read.parquet(TTFilePath)

    DFExcelUtil.saveDF2Excel(dfExcel,TTFilePath.replace("ThresholdTable.parquet",s"${mainCode.toLowerCase}.xlsx"))
  }
}
