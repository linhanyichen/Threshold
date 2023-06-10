package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.deep.threshold.dict.Dict
import org.deep.threshold.util.{DFExcelUtil, DFUtil, HDFSUtil, LogUtil, ShellUtil}

import scala.collection.mutable
/**
  * 阈值表计算服务类
  * create by zhuy 2019-11-15
  */
object ThresholdService {
  private var codeList:List[String] = List.empty
  private val codeMap:java.util.HashMap[String,java.util.HashMap[String,Any]] = new java.util.HashMap[String,java.util.HashMap[String,Any]]
  var spark:SparkSession = null
  var totalOrderCnt:Int = 0    // 总病历份数
  var calcedOrderCnt:Int = 0   // 已完成计算的病历份数
  var totalCodeCnt:Int = 0     // 主干组总个数
  var calcedCodeCnt:Int = 0    // 已完成计算的主干组个数

  private val allDataPath = s"hdfs://master:9000/threshold/data/base/all_sub_drg_data.parquet"
  private val basePath:String = s"${Dict.HDFSURL}/threshold/data/base/"

  // 主干组编码队列
  private [this] val codeQueue:mutable.Queue[String] = mutable.Queue.empty

  // 正在执行的主干组列表
  private [this] var codeSeq:mutable.Seq[String] = mutable.Seq.empty

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("ThresholdService")
      .master("local[1]")
      .getOrCreate()

    try{
      initCodeQueue()
      firstStart

      checkStatus
    }catch {
      case e:Exception => LogUtil.logERROR("阈值表生成处理失败,失败原因为:",e)
    }
  }


  /**
    * 初始化主干组队列
    */
  private def initCodeQueue() :Unit = {
    var df = spark.read.parquet("hdfs://master:9000/threshold/data/threshold_stat.parquet")
    df.createOrReplaceTempView("threshold_stat")
    df = spark.sql("select * from threshold_stat where `主干组编码` not in ('NONE_GROUP') order by `主干组编码` asc")
//    df = spark.sql("select * from threshold_stat where `主干组编码` not in ('FV2','IU2','ET2','OB1','RU1','ES2','BR2') order by `主干组编码` asc")
//    df = spark.sql("select * from threshold_stat order by `病例份数` asc")
    val dataList = getDF2List(df)

    dataList.toArray.foreach(c => {
      val map:java.util.HashMap[String,Any] = new java.util.HashMap[String,Any]()
      val col = c.asInstanceOf[java.util.HashMap[String,Object]]
      codeList ++= List(col.get("主干组编码").toString)

      map.put("cnt", col.get("病例份数").toString.toInt)
      map.put("finish", false)
      codeMap.put(col.get("主干组编码").toString, map)
      totalOrderCnt = totalOrderCnt + col.get("病例份数").toString.toInt
      totalCodeCnt = totalCodeCnt +1
    })

    codeList.foreach(code => codeQueue.enqueue(code))
    println(s"待执行主干组列表为：${codeList}")
    LogUtil.logINFO(s"共[${totalCodeCnt}]个主干组待执行,合计病历份数为[${totalOrderCnt}]")
  }

  /**
    * 首次启动三个主干组阈值表生成处理APP
    */
  private def firstStart() :Unit = {
    for (i<-0 to 2){
      val mainCode:String = codeQueue.dequeue()
      codeSeq ++= Seq(mainCode)
      startDRGThreshold(mainCode)
    }
  }

  /**
    * 启动mainCode主干组的阈值表生成处理程序
    * @param mainCode
    */
  private def startDRGThreshold(mainCode:String) :Unit = {

    if (HDFSUtil.isIniFileExist(s"${basePath}/${mainCode}/error.ini")
      || HDFSUtil.isIniFileExist(s"${basePath}/${mainCode}/finished.ini")
      || HDFSUtil.isIniFileExist(s"${basePath}/${mainCode}/nothreshold.ini")){
      return
    }

    if (HDFSUtil.isIniFileExist(s"${basePath}/${mainCode}/running.ini")) ShellUtil.exec(s"hdfs dfs -rm -r /threshold/base/${mainCode}")
    val shellStr:String = "spark-submit --class org.deep.threshold.RunThreshold --master spark://master:7077  " +
      "--deploy-mode cluster --driver-memory 5g --driver-cores 3 --executor-memory 5g --total-executor-cores 6 " +
      s"hdfs://master:9000/appjars/threshold.jar  --code ${mainCode} "

    ShellUtil.exec(shellStr)
    LogUtil.logINFO(s"启动[${mainCode}]阈值表生成处理App")
  }

  /**
    * 动态检测各主干组执行情况,并适时启动新的主干组阈值表处理APP
    */
  private def checkStatus() :Unit = {
    var bRun:Boolean = true

    var iLoop:Int = 0
    new Thread(new Runnable {
      override def run(): Unit = {
        while(bRun){

          var indexSeq:mutable.Seq[Int] = mutable.Seq.empty
          codeSeq.foreach(code => {
            // 存在错误异常终止的
            if(HDFSUtil.isIniFileExist(s"${Dict.HDFSURL}/threshold/data/base/${code}/nothreshold.ini")){
              calcedCodeCnt = calcedCodeCnt + 1
              calcedOrderCnt = calcedOrderCnt + codeMap.get(code).get("cnt").toString.toInt
              LogUtil.logINFO(s"主干组[${code}]阈值表生成处理已完成,当前已完成第[${calcedCodeCnt} / ${totalCodeCnt}]个主干组的阈值表计算,已完成[${(calcedOrderCnt*100.0/totalOrderCnt).formatted("%.5f")}%]的病历数据计算")
              indexSeq ++= Seq(codeSeq.indexOf(code))
            }
            // 存在错误异常终止的
            else if(HDFSUtil.isIniFileExist(s"${Dict.HDFSURL}/threshold/data/base/${code}/error.ini")){
              calcedCodeCnt = calcedCodeCnt + 1
              calcedOrderCnt = calcedOrderCnt + codeMap.get(code).get("cnt").toString.toInt
              LogUtil.logINFO(s"主干组[${code}]阈值表生成处理异常,请检查,当前已完成第[${calcedCodeCnt} / ${totalCodeCnt}]个主干组的阈值表计算,已完成[${(calcedOrderCnt*100.0/totalOrderCnt).formatted("%.5f")}%]的病历数据计算")
              indexSeq ++= Seq(codeSeq.indexOf(code))
            }
            // 阈值表生成结束
            else if(HDFSUtil.isIniFileExist(s"${Dict.HDFSURL}/threshold/data/base/${code}/finished.ini")){
              calcedCodeCnt = calcedCodeCnt + 1
              calcedOrderCnt = calcedOrderCnt + codeMap.get(code).get("cnt").toString.toInt
              LogUtil.logINFO(s"主干组[${code}]阈值表生成处理已完成,当前已完成第[${calcedCodeCnt} / ${totalCodeCnt}]个主干组的阈值表计算,已完成[${(calcedOrderCnt*100.0/totalOrderCnt).formatted("%.5f")}%]的病历数据计算")
              indexSeq ++= Seq(codeSeq.indexOf(code))
            }
          })

          // 更新正在执行的主干组列表
          if(!indexSeq.isEmpty) {
            indexSeq.foreach(index => {
              if (codeQueue.length > 0){
                val mainCode = codeQueue.dequeue()
                codeSeq(index) = mainCode
                startDRGThreshold(mainCode)
              }else{
                codeSeq(index) = ""
              }
            })
          }


          // 所有全部去执行完毕
          var flag:Boolean = true
          codeSeq.foreach(code => if (!code.equals("")) flag = false)

          if (flag){
            codeSeq = mutable.Seq.empty
            LogUtil.logINFO(s"DRG各主干组阈值表数据均已生成完毕,开始下载阈值表配置json文件")
            codeList.foreach(code => ShellUtil.exec(s"hdfs dfs -get /threshold/data/base/${code}/${code.toLowerCase}.json /deep/threshold_json/"))

            LogUtil.logINFO(s"阈值表配置json文件全部下载成功")
            LogUtil.logINFO(s"开始执行各DRG组权重值处理...")

            statAllData
            calcDRGWeigth

            LogUtil.logINFO(s"各DRG组权重值处理完毕,阈值表计算处理成功正常退出")

            bRun = false
            spark.stop()
          }

          iLoop = iLoop + 1
          Thread.sleep(1000); // 3秒检测一次
        }
      }
    }).start()

    LogUtil.logINFO("检测线程已启动")
  }

  private def statAllData() :Unit = {
    val total = codeList.size
    var setCnt:Int = 0

    codeList.foreach(code => {
      setCnt= setCnt + 1

      val statFilePath = s"hdfs://master:9000/threshold/data/base/${code}/sub_fee_data.parquet"

      var df = spark.read.parquet(statFilePath)
      df.createOrReplaceTempView(s"${code}_sub_fee_data")

      // 无子集划分的需补齐子集码
      if(spark.sql(s"select `DRG组编码`,count(`DRG组编码`) cnt from ${code}_sub_fee_data group by `DRG组编码`").count() == 1) {
        df = df.drop("DRG组编码")
        df = df.withColumn("DRG组编码", functions.lit(s"${code}9"))
      }

      // 汇总统计各DRG组病历份数和住院总费用
      df.createOrReplaceTempView(s"${code}_sub_fee_data")
      df = spark.sql(s"select `DRG组编码`,sum(`病历份数`) `病历份数`,sum(`费用均值`*`病历份数`) `住院总费用` from ${code}_sub_fee_data group by `DRG组编码`")

      // 追加
      if (HDFSUtil.isParquetFileExist(allDataPath)){
        df.write.mode(SaveMode.Append).parquet(allDataPath)
      }
      // 覆写
      else {
        df.write.mode(SaveMode.Overwrite).parquet(allDataPath)
      }

      if ((setCnt % math.floor(total/10.0) == 0))  LogUtil.logINFO(s"[主干组 -> ${code}]已完成[${(setCnt*100.0/total).formatted("%.2f").toString}%]的子集磁盘写入处理")
    })
  }

  /**
    * 计算各DRG组权重值
    */
  private def calcDRGWeigth() :Unit = {
    var dfRet = spark.read.parquet(allDataPath)
    dfRet = dfRet.withColumn("total_fee", functions.sum("住院总费用").over())
    dfRet = dfRet.withColumn("total_order", functions.sum("病历份数").over())

    dfRet.createOrReplaceTempView("ADRG_all_sub_drg_data")
    dfRet = spark.sql("select *,`住院总费用`/`病历份数` drg_fee_avg,total_fee/total_order all_fee_avg from ADRG_all_sub_drg_data")

    dfRet.createOrReplaceTempView("ADRG_all_sub_drg_data")
    dfRet = spark.sql("select *,drg_fee_avg/all_fee_avg weigth from ADRG_all_sub_drg_data")

    LogUtil.logINFO("各DRG组权重计算处理完毕,结果写入路径::/threshold/data/base/adrg_weigth.xlsx")
    DFExcelUtil.saveDF2Excel(dfRet,"hdfs://master:9000/threshold/data/base/adrg_weigth.xlsx")
  }


  protected def getDF2List(sDF:DataFrame) :java.util.List[java.util.HashMap[String,Object]] = {
    var fieldMap:Map[String,String]  = Map.empty
    sDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    DFUtil.DF2List(sDF,fieldMap)
  }

}
