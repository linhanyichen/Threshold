package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deep.threshold.proc.{KModelsParationProc, SubSetKMeansProc, SubSetParationProc, ThresholdTableProc}
import org.deep.threshold.util.LogUtil

import scala.actors.threadpool.{ExecutorService, Executors}

/**
  * 集群版阈值表计算处理类
  * create by zhuy 2019-11-06
  */
object RunThresholdSpark {

  var spark:SparkSession = null
  var mainCode:String = ""

  var executorService: ExecutorService = null

  private [this] var drgsCodeMap:java.util.HashMap[String,String] = new java.util.HashMap[String,String]()

  def main(args: Array[String]): Unit = {

    val codeIndex:Int = args.indexOf("--code")

    if (codeIndex > -1) {
      mainCode = args(codeIndex + 1)
    }


    val sparkConf = new SparkConf().setAppName(s"RunThresholdSpark-${mainCode}").set("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark = SparkSession.builder()
      .config(sparkConf)
      //                        .master("local[*]")
      .getOrCreate()


    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "dapPool")
    executorService = Executors.newFixedThreadPool(3)

    try{
      val running = Array("running")
      val runningRDD = spark.sparkContext.parallelize(running)
      runningRDD.saveAsTextFile(s"hdfs://master:9000/threshold/data/base/${mainCode}/running.ini")

      val df:DataFrame = spark.read.parquet(s"hdfs://master:9000/threshold/data/${mainCode}/trunk_group_data.parquet")

      LogUtil.logINFO(s"${mainCode} 主干组按诊断,手术子集划分处理开始")
      val sspp:SubSetParationProc  = new SubSetParationProc(spark)
      sspp.runSubSetParation(df,mainCode)
      LogUtil.logINFO(s"${mainCode} 主干组按诊断,手术子集划分处理结束")

      val kModelsProc:KModelsParationProc = new KModelsParationProc(spark)
      LogUtil.logINFO(s"开始处理${mainCode} 主干组Kmodels聚类")
      kModelsProc.runKModelsParation(mainCode)
      LogUtil.logINFO(s"${mainCode} 主干组Kmodels聚类处理结束")

      LogUtil.logINFO(s"${mainCode} 主干组SSKmeans聚类处理开始")
      val sSKmeansProc:SubSetKMeansProc = new SubSetKMeansProc(spark)
      sSKmeansProc.runSubSetKmeans(mainCode)

      drgsCodeMap = sSKmeansProc.getDrgsCodeMap()
      LogUtil.logINFO(s"${mainCode} 主干组SSKmeans聚类处理结束")

      LogUtil.logINFO(s"${mainCode} 主干组阈值表数据生成开始")
      val ttp:ThresholdTableProc = new ThresholdTableProc(spark)
      ttp.initExecutorService(executorService)
      ttp.runTTProc(mainCode,drgsCodeMap)

      LogUtil.logINFO(s"${mainCode} 主干组阈值表数据生成结束")

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
    executorService.shutdown()

    return
  }

}
