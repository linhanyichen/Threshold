package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deep.threshold.memproc.ThresholdTable2JSONProc
import org.deep.threshold.util.{DFUtil, LogUtil}

object GenThresholdJSON {

  var spark:SparkSession = null
  var mainCode:String = ""
  var filterCV:Double = 0.4   // 子集过滤用变异系数

  private [this] var drgsCodeMap:java.util.HashMap[String,String] = new java.util.HashMap[String,String]()

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

    try{

      LogUtil.logINFO(s"${mainCode} 主干组阈值表数据生成开始")
      val df = spark.read.parquet(s"hdfs://master:9000/threshold/data/base/${mainCode}/sub_fee_data.parquet")
      val list = getDF2List(df.select("`子集码`","`DRG组编码`"))

      list.toArray.foreach(row => {
        val data = row.asInstanceOf[java.util.HashMap[String, Object]]
        val drgCode = data.get("DRG组编码").toString
        val setCode = data.get("子集码").toString

        drgsCodeMap.put(setCode,drgCode)
      })

      val ttp:ThresholdTable2JSONProc = new ThresholdTable2JSONProc(spark)
      ttp.runTTProc(mainCode,drgsCodeMap)

      LogUtil.logINFO(s"${mainCode} 主干组阈值表数据生成结束")
      val finished = Array("finished")
      val finishedRDD = spark.sparkContext.parallelize(finished)
      finishedRDD.saveAsTextFile(s"hdfs://master:9000/threshold/data/base/${mainCode}/finished.ini")

    }catch {
      case e:Exception => {
//        val error = Array("error")
//        val finishedRDD = spark.sparkContext.parallelize(error)
//        finishedRDD.saveAsTextFile(s"hdfs://master:9000/threshold/data/base/${mainCode}/error.ini")
        LogUtil.logERROR("阈值表数据生成异常,错误信息如下:",e)
      }
    }

    spark.stop()

    return
  }

  /**
    * 将DF转成List[Map]
    * @param sDF
    * @return
    */
  protected def getDF2List(sDF:DataFrame) :java.util.List[java.util.HashMap[String,Object]] = {
    var fieldMap:Map[String,String]  = Map.empty
    sDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    DFUtil.DF2List(sDF,fieldMap)
  }

}
