package org.deep.threshold.proc

import java.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{HDFSUtil, LogUtil, ShellUtil, SubCodeUtil}

/**
  * 子集划分处理类
  */
class SubSetParationProc(spark:SparkSession) extends ProcTrait{

  private [this] var bContinue:Boolean = true                  // 计算过程是否继续标志,若此标志为false则直接赋值retDF后续操作不再处理
  private [this] var singleSubCode:String = ""                 // 所有主干组数据均被分到一个子集时的子集码
  private [this] var zDiagnosisList:util.List[String] = new util.LinkedList[String]()
  private [this] var surgeryList:util.List[String] = new util.LinkedList[String]()
  private [this] val finalSubCodeList:util.List[String] = new util.LinkedList[String]()


  private [this] var subGroupFeeDF:DataFrame = null            // 子集费用分析DF
  // 存储子集码和文件路径Map,key格式为主诊断|手术|辅诊断
  // {key -> {code -> 子集码, path -> 子集数据路径, temp -> 是否临时数据}}
  private [this] val propMap:util.HashMap[String,util.HashMap[String,String]] = new util.HashMap[String,util.HashMap[String,String]]()

  def runSubSetParation(sourceDF:DataFrame,mainCode:String) :Unit = {

    df = sourceDF
    this.mainCode = mainCode
    subFeeDataFilePath = Dict.subFeeDataPath.replace("&maincode",mainCode)
    dataSavePath = Dict.setDataPath.replace("&maincode",mainCode)

    // 生成子集费用分析表
    genSubGroupFeeDF

    // 以主诊断分子集
    parationByZDiagnosis

    // 相同主诊断下以手术分子集
    parationBySurgery

    // 相同主诊断、手术下以辅诊断分子集
    parationByFDiagnosis

    // 获取有效子集码信息,并更新子集费用分析表
    updSubFeeData
    LogUtil.logWARN(s"[主干组->${mainCode}]源数据共被分为[${finalSubCodeList.size()}]个子集")
  }

  /**
    * 是否为单个子集数据
    * @return true-所有主干组数据均为单个子集,false-非单个子集
    */
  def isSingleSubSet() : Boolean = {
    !bContinue
  }

  /**
    * 获取单子集码
    * @return
    */
  def getSingleSubCode() : String = {
    singleSubCode
  }

  /**
    * 有效子集码列表
    * @return
    */
  def getSubCodeList() :util.List[String] = {
    finalSubCodeList
  }


  /**
    * 生成子集费用分析表
    */
  private def genSubGroupFeeDF():Unit = {
    val subCode = SubCodeUtil.getSubCode() +"-1"
    subGroupFeeDF = df.selectExpr("avg(`医疗费用`) as `费用均值`","percentile(`医疗费用`,0.5) as `中位数`",
      "stddev_pop(`医疗费用`) as `标准差`","stddev_pop(`医疗费用`)/avg(`医疗费用`) as `变异系数`",
      "max(`医疗费用`) as `最高费用`","min(`医疗费用`) as `最低费用`","count(`病历序号`) as `病历份数`",
      s"'${subCode}' as `子集码`", "'' as `DRG组编码`", "0.00 as `DRG组费用标准`").toDF()

    singleSubCode = subCode

    val savePath = s"${dataSavePath}${subCode}.parquet"
    val prop:util.HashMap[String,String] = new util.HashMap[String,String]()
    prop.put("code", subCode)        // 子集码
    prop.put("path", savePath)       // 文件路径
    prop.put("temp", "true")         // 临时文件
    propMap.put(mainCode, prop)

    subGroupFeeDF.write.mode(SaveMode.Overwrite).parquet(subFeeDataFilePath)
    LogUtil.logINFO(s"[${mainCode}]主干组子集费用分析数据写HDFS")
  }


  /**
    * 以主诊断分子集
    */
  private def parationByZDiagnosis() :Unit = {

    // 主干组子集不满足(病历份数>10 and 变异系数 > 0.4)时,不再做处理
    if (subGroupFeeDF.filter("`病历份数` > 10").filter("`变异系数` >= 0.4").count() == 0){
      LogUtil.logINFO(s"[主干组->${mainCode}]子集不满足(病历份数>10 and 变异系数 > 0.4)不再进行细分处理)")

      //更新子集码
      if (df.columns.contains("子集码")) df = df.drop(df("子集码"))
      df = df.withColumn("子集码", functions.lit(propMap.get(mainCode).get("code").toString))
      df.write.mode(SaveMode.Overwrite).parquet(propMap.get(mainCode).get("path").toString)
      propMap.get(mainCode).put("temp","false")
      bContinue = false
      return
    }

    HDFSUtil.deleteFile(propMap.get(mainCode).get("path").toString)

    // 获取主诊断列表
    zDiagnosisList = getDFValueList(df.groupBy("`主诊断`").count(), "主诊断")
    LogUtil.logINFO(s"[主干组 -> ${mainCode}]共需处理[${zDiagnosisList.size()}]个主诊断子集")

    zDiagnosisList.toArray.foreach(zd => {
      val diagnosis = zd.toString
      val subCode = SubCodeUtil.getSubCode()+"-1ZZD"
      var diagnosisDF = df.filter(s"`主诊断` = '${diagnosis}'")

      // 追加子集费用分析表
      appendSubFeeData(diagnosisDF,subCode)

      // 更新子集码
      if (diagnosisDF.columns.contains("子集码")) diagnosisDF = diagnosisDF.drop("子集码")
      diagnosisDF = diagnosisDF.withColumn("子集码", functions.lit(subCode))

      // 保存子集码信息
      val savePath = s"${dataSavePath}${subCode}.parquet"
      val prop:util.HashMap[String,String] = new util.HashMap[String,String]()
      prop.put("code", subCode)    // 子集码
      prop.put("path", savePath)   // 文件路径
      prop.put("temp", "true")     // 临时文件

      diagnosisDF.write.mode(SaveMode.Overwrite).parquet(savePath)
      propMap.put(diagnosis,prop)
    })

    LogUtil.logINFO(s"[主干组 -> ${mainCode}]按主诊断分解子集完毕")
    subGroupFeeDF = spark.read.parquet(subFeeDataFilePath)
  }

  /**
    * 各主诊断以手术划分子集
    */
  private def parationBySurgery() :Unit = {
    if(!bContinue) return

    zDiagnosisList.toArray.foreach(zd => {
      val zdFilePath = propMap.get(zd.toString).get("path")
      val zDiagnosisDF = spark.read.parquet(zdFilePath)
      val zdCode = propMap.get(zd.toString).get("code")

      // 主诊断子集是否满足 病例份数 > 10 且 变异系数 > 0.4
      val subFeeCnt = subGroupFeeDF.where(s"`子集码` = '${zdCode}' and `病历份数` > 10 and `变异系数` >= 0.4").count()

      //存在子集属性满足下步计算时
      if (subFeeCnt > 0){
        val hasSurgeryDF = zDiagnosisDF.filter("`手术` is not null and `手术` != ''")
        val keyNoSurgery :String = zd.toString+"|null"  // 手术为空时propMap的key
        var bProcNoSurgery :Boolean = false
        // 存在手术不为空的数据
        if (hasSurgeryDF.count() > 0){

          // 存在手术为空的数据,则保存手术为空的数据
          if (hasSurgeryDF.count() != zDiagnosisDF.count()){
            bProcNoSurgery = true
            val noSurgeryDF = zDiagnosisDF.where("`手术` is null or `手术` = ''")
            val subCode = SubCodeUtil.getSubCode()+"-1SSK"
            val savePath = s"${dataSavePath}${subCode}.parquet"
            val prop:util.HashMap[String,String] = new util.HashMap[String,String]()
            prop.put("code", subCode)        // 子集码
            prop.put("path", savePath)       // 文件路径
            prop.put("temp", "false")         // 临时文件

            noSurgeryDF.write.mode(SaveMode.Overwrite).parquet(savePath)
            propMap.put(keyNoSurgery,prop)
            propMap.get(zd.toString).put("temp", "true")

            // 追加子集费用分析表
            appendSubFeeData(noSurgeryDF,subCode)
          }

          surgeryParationProc(hasSurgeryDF,zd.toString)

          // 删除临时数据
          HDFSUtil.deleteFile(zdFilePath.substring(18))
          LogUtil.logDEBUG(s"[主干组 -> ${mainCode}][主诊断 -> ${zd.toString}]子集下所有手术数据划分完毕,清除临时数据[${zdFilePath.substring(18)}]")
        }else{
          // add by zhuy 2019-11-18 for 手术为空时需按辅诊断继续划分子集
          surgeryList.add(s"${zd.toString}")
        }

      }else {
        // 子集属性不满足继续计算时,更新当前子集为非临时数据表
        propMap.get(zd.toString).put("temp", "false")
      }
    })

    subGroupFeeDF = spark.read.parquet(subFeeDataFilePath)
  }

  /**
    * 相同主诊断下的手术子集划分处理
    * @param zDiagnosisDF 主诊断对应的数据集
    * @param zd 主诊断
    */
  private def surgeryParationProc(zDiagnosisDF:DataFrame,zd:String) :Unit = {
    // 获取手术列表
    val sList = getDFValueList(zDiagnosisDF.groupBy(s"`手术`").count(),"手术")

    sList.toArray.foreach(s => {
      val surgery = s.toString
      val subCode = SubCodeUtil.getSubCode()+"-1SS"
      var surgeryDF = zDiagnosisDF.filter(s"`手术` = '${surgery}'")

      // 追加子集费用分析表
      appendSubFeeData(surgeryDF,subCode)

      // 更新子集码
      surgeryDF = surgeryDF.drop("子集码")
      surgeryDF = surgeryDF.withColumn("子集码", functions.lit(subCode))

      // 保存子集码信息
      val savePath = s"${dataSavePath}${subCode}.parquet"
      val prop:util.HashMap[String,String] = new util.HashMap[String,String]()
      prop.put("code", subCode)    // 子集码
      prop.put("path", savePath)   // 文件路径
      prop.put("temp", "true")     // 临时文件

      surgeryDF.write.mode(SaveMode.Overwrite).parquet(savePath)
      propMap.put(s"${zd}|${surgery}",prop)

      // 以'主诊断|手术'形式拼接key
      surgeryList.add(s"${zd}|${surgery}")
    })

    LogUtil.logINFO(s"[主干组 -> ${mainCode}][主诊断 -> ${zd}]按手术分解子集处理完毕")
  }

  /**
    * 以辅诊断划分子集
    */
  private def parationByFDiagnosis() :Unit = {
    if(!bContinue) return

    surgeryList.toArray.foreach(s => {
      val key = s.toString
      val subCode = propMap.get(key).get("code").toString
      val filePath = propMap.get(key).get("path").toString
      val surgeryDF = spark.read.parquet(filePath)

      val subGroupFeeCnt = subGroupFeeDF.where(s"`子集码` = '${subCode}' and `病历份数` > 10 and `变异系数` >= 0.4").count()
      if(subGroupFeeCnt > 0){
        FDiagnosisParationProc(surgeryDF,key)

        if (propMap.get(key).get("temp").equals("true")){
          // 删除手术子集数据
          HDFSUtil.deleteFile(filePath.substring(18))
        }
      }else{
        // 子集属性不满足继续计算时,更新当前子集为非临时数据表
        LogUtil.logDEBUG(s"[主干组 -> ${mainCode}][主诊断|手术 -> ${key}]子集[${propMap.get(key).get("code")}]属性不满足继续计算,更新当前子集为非临时数据表")
        propMap.get(key).put("temp", "false")
      }
    })

    subGroupFeeDF = spark.read.parquet(subFeeDataFilePath)
  }


  /**
    * 同一手术下的副诊断子集划分处理
    * @param surgeryDF 相同手术的数据集
    * @param surgery 主诊断|手术
    */
  private def FDiagnosisParationProc(surgeryDF:DataFrame ,surgery:String) :Unit = {
    val FDiagnosisList = getDFValueList(surgeryDF.groupBy(s"`辅诊断`").count(),"辅诊断")

    // 只有一个辅诊断,就不再进行子集划分
    if(FDiagnosisList.size() == 1){
      propMap.get(surgery).put("temp","false")
      LogUtil.logDEBUG(s"[主干组 -> ${mainCode}][主诊断|手术 -> ${surgery}]下所有辅诊断均相同,不再进行子集划分")
      return
    }

    FDiagnosisList.toArray.foreach(fd => {
      val FDiagnosisStr:String = fd.toString
      var FDiagnosisDF :DataFrame = null
      val subCode = SubCodeUtil.getSubCode()+"-1FZD"

      if (FDiagnosisStr.equals(""))   FDiagnosisDF = surgeryDF.where("`辅诊断` is null or `辅诊断` = ''")
      else                            FDiagnosisDF = surgeryDF.filter(s"`辅诊断` = '${FDiagnosisStr}'")

      // 追加子集费用分析表
      appendSubFeeData(FDiagnosisDF,subCode)

      // 更新子集码
      FDiagnosisDF = FDiagnosisDF.drop("子集码")
      FDiagnosisDF = FDiagnosisDF.withColumn("子集码", functions.lit(subCode))

      // 保存子集码信息
      val savePath = s"${dataSavePath}${subCode}.parquet"
      val prop:util.HashMap[String,String] = new util.HashMap[String,String]()
      prop.put("code", subCode)    // 子集码
      prop.put("path", savePath)   // 文件路径
      prop.put("temp", "false")    // 临时文件

      FDiagnosisDF.write.mode(SaveMode.Overwrite).parquet(savePath)
      propMap.put(s"${surgery}|${FDiagnosisStr}",prop)
    })
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
      s"'${subCode}' as `子集码`", "'' as `DRG组编码`", "0.00 as `DRG组费用标准`").toDF()

    dfTmp.write.mode(SaveMode.Append).parquet(subFeeDataFilePath)
  }

  /**
    * 更新子集数据信息
    */
  private def updSubFeeData() :Unit = {

    var whereSQL :String = "`子集码` in ( "
    propMap.keySet().toArray.foreach(key => {

      val keyStr = key.toString
      if (propMap.get(keyStr).get("temp").equals("false")){
        whereSQL += s" '${propMap.get(keyStr).get("code")}',"
        finalSubCodeList.add(propMap.get(keyStr).get("code"))
      }
    })

    whereSQL = whereSQL.substring(0,whereSQL.length-1) + " )"

    subGroupFeeDF = subGroupFeeDF.where(whereSQL).repartition(2)
    subGroupFeeDF = subGroupFeeDF.na.fill(0.0)  // 将所有Nan值更新为0,以便后续判断变异系数值
    subGroupFeeDF.write.mode(SaveMode.Overwrite).parquet(subFeeDataFilePath.replace("sub_fee_data","sub_fee_data_bak"))
    ShellUtil.exec(s"hdfs dfs -rm -r ${subFeeDataFilePath.substring(18)}")
    ShellUtil.exec(s"hdfs dfs -mv ${subFeeDataFilePath.replace("sub_fee_data","sub_fee_data_bak").substring(18)} ${subFeeDataFilePath.substring(18)}")
  }

}
