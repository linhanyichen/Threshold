package org.deep.threshold.memproc

import java.util

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{LogUtil, StatUtil, SubCodeUtil}

/**
  * 诊断,手术子集划分处理(内存版本)
  * create by zhuy 2019-11-18
  */
class SSParationProc (spark:SparkSession) extends ProcTrait{

  // 主干组数据内存存储Map
  private [this] val dataMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()

  // 子集费用分析表
  private [this] val subFeeMap:util.HashMap[String,util.HashMap[String,Object]] = new util.HashMap[String,util.HashMap[String,Object]]()

  // 主诊断列表
  private [this] var zzdList:util.List[String] = new util.LinkedList[String]()

  // 手术子集列表
  private [this] var ssList:List[String] = List.empty

  private [this] var bContinue:Boolean = true
  private [this] var bNeenCaclThreshold:Boolean = true   // 是否需要生成阈值表
  private [this] var filterCV:Double = _


  var schema:StructType = null
  var feeSchema:StructType = null

  /**
    * 按主诊断、手术、辅诊断划分子集
    * @param sourceDF
    * @param mainCode
    * @param cv
    */
  def runSSParation(sourceDF:DataFrame,mainCode:String,cv:Double) :Unit = {

    df = sourceDF
    this.mainCode = mainCode
    filterCV = cv
    subFeeDataFilePath = Dict.subFeeDataPath.replace("&maincode",mainCode)
    dataSavePath = Dict.setDataPath.replace("&maincode",mainCode)

    schema = df.schema

    // 将数据加载到内存中
    dataLoad

    // 计算主干组整体子集费用信息
    calcAllData
    if(!bContinue) return

    // 按主诊断划分子集
    parationByZZD

    // 按手术划分子集
    parationBySS()

    // 按辅诊断划分子集
    parationByFZD

    LogUtil.logINFO(s"[主干组 -> ${mainCode}]共被分为[${subFeeMap.keySet().size()}]个子集")
  }

  def getDataMap() :util.HashMap[String,util.HashMap[String,Object]] = {
    dataMap
  }

  def getSubFeeMap() :util.HashMap[String,util.HashMap[String,Object]] = {
    subFeeMap
  }

  /**
    * 是否需要进行阈值表计算处理
    * @return
    */
  def needCalcThreshold() :Boolean = {
    bNeenCaclThreshold
  }

  /**
    * 将数据加载到内存中
    */
  private def dataLoad() :Unit = {
    var fieldMap:Map[String,String]  = Map.empty
    df.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    val retRows = df.collect()
    retRows.foreach(row => {
      val order_id:String = row.getAs("病历序号").toString
      val zzd:String = row.getAs("主诊断").toString
      val ss:String = row.getAs("手术").toString
      val fzd:String = row.getAs("辅诊断").toString
      val fee:Double = row.getAs("医疗费用").toString.toDouble
      val attrMap:util.HashMap[String,Object] = new util.HashMap[String,Object]()
      attrMap.put("set_code","")
      attrMap.put("row", row.asInstanceOf[Object])
      attrMap.put("fee",fee.asInstanceOf[Object])

      val key = s"${order_id}|${zzd}|${ss}|${fzd}"
      dataMap.put(s"${order_id}|${zzd}|${ss}|${fzd}",attrMap)
    })
  }


  /**
    * 计算所有费用变异系数信息
    */
  private def calcAllData() :Unit = {
    val subCode = SubCodeUtil.getSubCode() +"-1"
    subGroupFeeDF = df.selectExpr("avg(`医疗费用`) as `费用均值`","percentile(`医疗费用`,0.5) as `中位数`",
      "stddev_pop(`医疗费用`) as `标准差`","stddev_pop(`医疗费用`)/avg(`医疗费用`) as `变异系数`",
      "max(`医疗费用`) as `最高费用`","min(`医疗费用`) as `最低费用`","count(`病历序号`) as `病历份数`",
      s"'${subCode}' as `子集码`", "'' as `DRG组编码`").toDF()

    feeSchema = subGroupFeeDF.schema

    val ret = getDF2List(subGroupFeeDF)
    val cv :Double = ret.get(0).get("变异系数").toString.toDouble
    val orderCnt:Int = ret.get(0).get("病历份数").toString.toDouble.toInt

    if(!(cv >= filterCV && orderCnt > 10)){
      val path:String = Dict.setDataPath.replaceAll("&maincode", mainCode)+ subCode+".parquet"
      subGroupFeeDF.write.mode(SaveMode.Overwrite).parquet(subFeeDataFilePath)
      df = df.withColumn("子集码",functions.lit(s"${subCode}"))
      df.write.mode(SaveMode.Overwrite).parquet(path)
      LogUtil.logINFO(s"[主干组 -> ${mainCode}]变异系数[${cv}],病历份数[${orderCnt}]不满足子集细分处理条件,不再进行子集划分处理,无需计算阈值表")

      val nothreshold = Array("nothreshold")
      val finishedRDD = spark.sparkContext.parallelize(nothreshold)
      finishedRDD.saveAsTextFile(s"hdfs://master:9000/threshold/data/base/${mainCode}/nothreshold.ini")
      bNeenCaclThreshold = false
      bContinue = false
    }
  }

  /**
    * 按主诊断进行子集划分
    */
  private def parationByZZD() :Unit = {

    zzdList = getDFValueList(df.groupBy("`主诊断`").count(),"主诊断")
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]按主诊断划分子集处理开始,共[${zzdList.size()}]个")

    var codeList:List[String] = List.empty


    // 单个主诊断的特殊处理,以提升计算性能  add by zhuy 2020-01-03
    if (zzdList.size() == 1){
      val zzd = zzdList.get(0)
      var rowSeq:Seq[Row] = Seq.empty
      val setCode:String = SubCodeUtil.getSubCode()+"-1ZZD"
      var feeList:List[Double] = List.empty
      var orderCnt:Long = 0
      codeList ++= List(setCode)

      dataMap.keySet().toArray.foreach(k => {
        val key = k.toString
        dataMap.get(key).put("set_code", setCode)
        rowSeq ++= Seq(dataMap.get(key).get("row").asInstanceOf[Row])
        feeList ++= List(dataMap.get(key).get("fee").toString.toDouble)
        orderCnt = orderCnt + 1
      })

      // 计算费用均值,中位数,标准差,变异系数,最高费用,最低费用,病历份数
      val propMap:util.HashMap[String,Object] = new util.HashMap[String,Object]()
      propMap.put("费用均值", StatUtil.avg(feeList).asInstanceOf[Object])
      propMap.put("中位数", StatUtil.median(feeList).asInstanceOf[Object])
      propMap.put("标准差", StatUtil.stdev(feeList).asInstanceOf[Object])
      propMap.put("变异系数", StatUtil.cv(feeList).asInstanceOf[Object])
      propMap.put("最高费用", StatUtil.max(feeList).asInstanceOf[Object])
      propMap.put("最低费用", StatUtil.min(feeList).asInstanceOf[Object])
      propMap.put("病历份数", orderCnt.asInstanceOf[Object])
      propMap.put("主诊断", zzd.toString)
      propMap.put("手术", "-")
      propMap.put("辅诊断", "-")

      subFeeMap.put(setCode,propMap)
    }else {
      zzdList.toArray.foreach(zzd => {
        var rowSeq:Seq[Row] = Seq.empty
        val setCode:String = SubCodeUtil.getSubCode()+"-1ZZD"
        var feeList:List[Double] = List.empty
        var orderCnt:Long = 0
        codeList ++= List(setCode)

        dataMap.keySet().toArray.foreach(k => {
          val key = k.toString
          val zzdStr:String = key.split('|')(1)
          if (zzdStr.equals(zzd.toString) && dataMap.get(key).get("set_code").toString.equals("")) {
            dataMap.get(key).put("set_code", setCode)
            rowSeq ++= Seq(dataMap.get(key).get("row").asInstanceOf[Row])
            feeList ++= List(dataMap.get(key).get("fee").toString.toDouble)
            orderCnt = orderCnt + 1
          }
        })

        // 计算费用均值,中位数,标准差,变异系数,最高费用,最低费用,病历份数
        val propMap:util.HashMap[String,Object] = new util.HashMap[String,Object]()
        propMap.put("费用均值", StatUtil.avg(feeList).asInstanceOf[Object])
        propMap.put("中位数", StatUtil.median(feeList).asInstanceOf[Object])
        propMap.put("标准差", StatUtil.stdev(feeList).asInstanceOf[Object])
        propMap.put("变异系数", StatUtil.cv(feeList).asInstanceOf[Object])
        propMap.put("最高费用", StatUtil.max(feeList).asInstanceOf[Object])
        propMap.put("最低费用", StatUtil.min(feeList).asInstanceOf[Object])
        propMap.put("病历份数", orderCnt.asInstanceOf[Object])
        propMap.put("主诊断", zzd.toString)
        propMap.put("手术", "-")
        propMap.put("辅诊断", "-")

        subFeeMap.put(setCode,propMap)
      })
    }

    LogUtil.logINFO(s"[主干组 -> ${mainCode}]按主诊断划分子集处理完毕")
  }

  /**
    * 按手术进行子集划分
    */
  private def parationBySS() :Unit = {

    var delCodeList:List[String] = List.empty   // 待删除的子集码列表

    subFeeMap.keySet().toArray.foreach(key => {
      val code = key.toString

      // 获取变异系数和病历份数
      val cv:Double = subFeeMap.get(code).get("变异系数").toString.toDouble
      val orderCnt:Int = subFeeMap.get(code).get("病历份数").toString.toDouble.toInt

      // 变异系数大于等于0.4且病历份数大于10的才做处理
      if(cv >= filterCV && orderCnt > 10){
        val zzd = subFeeMap.get(code).get("主诊断").toString
        // 按主诊断依次获取主诊断下的手术列表
        val ssLists = getDFValueList(df.filter(s"`主诊断` = '${zzd}'").groupBy("`手术`").count(),"手术")

        // 主诊断下所有手术均为空
        if(ssLists.size() == 1 && ssLists.get(0).toString.trim.equals("")){
          ssList ++= List(zzd.toString+"||")
          subFeeMap.get(code).put("手术","")
        }else {
          // 计算各主诊断下的手术子集
          ssLists.toArray.foreach(ss => {
            val setCode = SubCodeUtil.getSubCode()+"-1SS"
            val matchStr = s"${zzd}|${ss.toString}|"
            ssList ++= List(matchStr)

            var rowSeq:Seq[Row] = Seq.empty
            var feeList:List[Double] = List.empty
            var orderCnt:Long = 0

            dataMap.keySet().toArray.foreach(k => {
              val key = k.toString
              val keySplit = key.split('|')
              var compStr:String = ""

              //无手术和辅诊断的情况
              if(keySplit.size == 2)  compStr = keySplit(1)+"||"
              else compStr = keySplit(1) + "|" + keySplit(2) + "|"

              // 该子集病历更新子集码
              if (compStr.equals(matchStr)){
                dataMap.get(key).put("set_code", setCode)
                rowSeq ++= Seq(dataMap.get(key).get("row").asInstanceOf[Row])
                feeList ++= List(dataMap.get(key).get("fee").toString.toDouble)
                orderCnt = orderCnt + 1
              }
            }) // end of while

            // 计算费用均值,中位数,标准差,变异系数,最高费用,最低费用,病历份数
            val propMap:util.HashMap[String,Object] = new util.HashMap[String,Object]()
            propMap.put("费用均值", StatUtil.avg(feeList).asInstanceOf[Object])
            propMap.put("中位数", StatUtil.median(feeList).asInstanceOf[Object])
            propMap.put("标准差", StatUtil.stdev(feeList).asInstanceOf[Object])
            propMap.put("变异系数", StatUtil.cv(feeList).asInstanceOf[Object])
            propMap.put("最高费用", StatUtil.max(feeList).asInstanceOf[Object])
            propMap.put("最低费用", StatUtil.min(feeList).asInstanceOf[Object])
            propMap.put("病历份数", orderCnt.asInstanceOf[Object])
            propMap.put("主诊断", zzd.toString)
            propMap.put("手术", ss.toString)
            propMap.put("辅诊断", "-")

            subFeeMap.put(setCode,propMap)
          })  // end of foreach

          // 删除主诊断对应的子集费用数据信息
          delCodeList ++= List(code)
        } // end of else
      } //end of if
    }) // end of foreach

    //删除无效子集费用信息
    delCodeList.foreach(code => subFeeMap.remove(code))
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]按手术划分子集处理完毕")
  }


  /**
    * 按照辅诊断进行子集划分
    */
  private def parationByFZD() :Unit = {

    var delCodeList:List[String] = List.empty   // 待删除的子集码列表
    subFeeMap.keySet().toArray.foreach(key => {
      val code = key.toString

      // 提取满足继续计算的子集信息
      val zzd = subFeeMap.get(code).get("主诊断").toString
      val ss  = subFeeMap.get(code).get("手术").toString
      val cv:Double = subFeeMap.get(code).get("变异系数").toString.toDouble
      val orderCnt:Int = subFeeMap.get(code).get("病历份数").toString.toDouble.toInt

      // 存在于待处理的子集且变异系数大于等于0.4且病历份数大于10
      if(!ss.equals("-") && ssList.indexOf(s"${zzd}|${ss}|") != -1
        && (cv >= filterCV && orderCnt > 10)){

        var fzdList:util.List[String] = new util.LinkedList[String]()
        if (ss.isEmpty) fzdList = getDFValueList(df.filter(s"`主诊断` = '${zzd}' and (`手术` is null or `手术` = '')").groupBy("`辅诊断`").count(),"辅诊断")
        else fzdList = getDFValueList(df.where(s"`主诊断` = '${zzd}' and `手术` = '${ss}'").groupBy("`辅诊断`").count(),"辅诊断")

        fzdList.toArray.foreach(fzd => {
          val fzdStr :String = s"|${zzd}|${ss}|${fzd.toString}"

          var rowSeq:Seq[Row] = Seq.empty
          var feeList:List[Double] = List.empty
          var orderCnt:Long = 0
          val setCode = SubCodeUtil.getSubCode()+"-1FZD"

          dataMap.keySet().toArray.foreach(k => {
            val key = k.toString
            // 该子集病历更新子集码
            if (key.endsWith(fzdStr)){
              dataMap.get(key).put("set_code", setCode)

              rowSeq ++= Seq(dataMap.get(key).get("row").asInstanceOf[Row])
              feeList ++= List(dataMap.get(key).get("fee").toString.toDouble)
              orderCnt = orderCnt + 1
            }
          }) // end of while

          // 计算费用均值,中位数,标准差,变异系数,最高费用,最低费用,病历份数
          val propMap:util.HashMap[String,Object] = new util.HashMap[String,Object]()
          propMap.put("费用均值", StatUtil.avg(feeList).asInstanceOf[Object])
          propMap.put("中位数", StatUtil.median(feeList).asInstanceOf[Object])
          propMap.put("标准差", StatUtil.stdev(feeList).asInstanceOf[Object])
          propMap.put("变异系数", StatUtil.cv(feeList).asInstanceOf[Object])
          propMap.put("最高费用", StatUtil.max(feeList).asInstanceOf[Object])
          propMap.put("最低费用", StatUtil.min(feeList).asInstanceOf[Object])
          propMap.put("病历份数", orderCnt.asInstanceOf[Object])
          propMap.put("主诊断", zzd.toString)
          propMap.put("手术", ss.toString)
          propMap.put("辅诊断", "-")
          subFeeMap.put(setCode,propMap)
        }) //enf of foreach

        // 删除主诊断对应的子集费用数据信息
        delCodeList ++= List(code)

      } // end of if
    }) // end of foreach

    //删除无效子集费用信息
    delCodeList.foreach(code => subFeeMap.remove(code))
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]按辅诊断划分子集处理完毕")
  }
}
