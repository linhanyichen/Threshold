package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deep.threshold.memproc.{KmodesParationProc, SSKmeansProc, SSParationProc, ThresholdTable2JSONProc}
import org.deep.threshold.util.LogUtil

import java.util.HashMap

/**
  * 按主干组执行阈值表计算处理
  */
object RunThreshold {

  var spark:SparkSession = null
  var mainCode:String = ""
  var filterCV:Double = 0.4   // 子集过滤用变异系数
  var cvList:List[Double] = List.empty   // 亚组划分变异系数参数

  private [this] var drgsCodeMap:HashMap[String,String] = new HashMap[String,String]()
  private [this] var ssp:SSParationProc = null
  private [this] var kmodes:KmodesParationProc = null

  def main(args: Array[String]): Unit = {

    val codeIndex:Int = args.indexOf("--code")

    if (codeIndex > -1) {
      mainCode = args(codeIndex + 1)
    }
    val sparkConf = new SparkConf().setAppName(s"RunThreshold-${mainCode}")
    /*.set("spark.scheduler.mode", "FAIR")*/
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark = SparkSession.builder()
      .config(sparkConf)
      //                        .master("local[*]")
      .getOrCreate()

    // 获取filterCV值
    // cv_config.ini 格式  key=value
    val cvValue = spark.read.textFile("hdfs://master:9000/threshold/config/cv_config.ini")
    val cvRet = cvValue.collect()
    filterCV = cvRet(0).split('=')(1).toDouble
    LogUtil.logINFO(s"第一阶段子集过滤所用变异系数比较值为::$filterCV")

    // 获取亚组划分变异系数参数值
    if (cvRet.size > 1){
      val CVs = cvValue.collect()(1).split('=')(1).split('|')
      CVs.toList.foreach(cvValue => cvList ++= List(cvValue.toDouble))
    }

    ssp = new SSParationProc(spark)
    kmodes = new KmodesParationProc(spark)

    try{
      val running = Array("running")
      val runningRDD = spark.sparkContext.parallelize(running)
      runningRDD.saveAsTextFile(s"hdfs://master:9000/threshold/data/base/${mainCode}/running.ini")

      val df:DataFrame = spark.read.parquet(s"hdfs://master:9000/threshold/data/${mainCode}/dat_order.parquet")

      LogUtil.logINFO(s"${mainCode} 主干组按诊断,手术子集划分处理开始")
      ssp.runSSParation(df,mainCode,filterCV)

      // 满足阈值表计算时才进行相关计算处理
      if (ssp.needCalcThreshold()){
        kmodes.initSchema(ssp.schema,ssp.feeSchema)
        kmodes.dataInit(ssp.getDataMap(),ssp.getSubFeeMap())
        kmodes.runKmodesParationProc(mainCode)

        LogUtil.logINFO(s"${mainCode} 主干组SSKmeans聚类处理开始")

        val ssKmeans:SSKmeansProc = new SSKmeansProc(spark)
        ssKmeans.initSchema(ssp.schema,ssp.feeSchema)
        if (!cvList.isEmpty) ssKmeans.initCVs(cvList)
        ssKmeans.runSSKmeansProc(kmodes.getDataMap(),kmodes.getSubFeeMap(),mainCode)
        drgsCodeMap = ssKmeans.getDrgsCodeMap()
        LogUtil.logINFO(s"${mainCode} 主干组SSKmeans聚类处理结束")

        if (ssKmeans.getGroupNumber() > 1 && ssKmeans.subSetNumber > 1){
          LogUtil.logINFO(s"${mainCode} 主干组阈值表数据生成开始")
          val ttp:ThresholdTable2JSONProc = new ThresholdTable2JSONProc(spark)
          ttp.runTTProc(mainCode,drgsCodeMap)

          LogUtil.logINFO(s"${mainCode} 主干组阈值表数据生成结束")
        }else {
          LogUtil.logINFO(s"${mainCode} 主干组所有有效病例均属于同一子集,所有病例亚组编码均为[${mainCode}9]无需生成阈值表")
        }
      }

      val finished = Array("finished")
      val finishedRDD = spark.sparkContext.parallelize(finished)
      finishedRDD.saveAsTextFile(s"hdfs://master:9000/threshold/data/base/${mainCode}/finished.ini")

    }catch {
      case e:Exception => {
        val error = Array("error")
        val finishedRDD = spark.sparkContext.parallelize(error)
        finishedRDD.saveAsTextFile(s"hdfs://master:9000/threshold/data/base/${mainCode}/error.ini")
        LogUtil.logERROR("阈值表数据生成异常,错误信息如下:",e)
      }
    }

    spark.stop()

    return
  }

}
