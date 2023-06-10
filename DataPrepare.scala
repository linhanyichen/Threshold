package org.deep.threshold

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.deep.threshold.util.{DFUtil, HDFSUtil, LogUtil, ShellUtil}

/**
  * 数据预处理类,将全集数据按照主干组分别存储
  * create by zhuy 2019-11-14
  */
object DataPrepare {

  var spark:SparkSession = null
  private [this] var colMaps:Map[String,String] = Map.empty
  private [this] var mainCodeList:List[String] = List.empty

  val schema = StructType(List(
    StructField("主干组编码", StringType, nullable = true),
    StructField("病例份数", IntegerType, nullable = true),
    StructField("有效病例份数", IntegerType, nullable = true),
    StructField("亚组个数", IntegerType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("PrepareData")
      //      .master("local[*]")
      .getOrCreate()

    init


    var df: DataFrame = spark.read.parquet("hdfs://master:9000/dap/projs/threshold/data/source/dat_order_prepare.parquet")
    df.createOrReplaceTempView("dat_order_prepare")

    df = spark.sql("select * from dat_order_prepare where zzd is not null and zzd != ''")   // 剔除主诊断为空的数据
    val codeList = getDFValueList(df.groupBy("ADRG").count(), "ADRG")

    val total :Int = codeList.size()
    LogUtil.logINFO(s"共计[${total}]个主干组数据")

    codeList.toArray.foreach(code => {if(!code.toString.equals("NONE_GROUPED")) mainCodeList ++= List(code.toString) })

    //  字段名转换处理
    colMaps.keySet.toList.foreach(oldCol => {
      df = df.withColumnRenamed(oldCol, colMaps.get(oldCol).get)
    })

    var rowSeq:Seq[Row] = Seq.empty
    var cnt:Int = 1
    mainCodeList.foreach(mainCode => {

      var drgDF:DataFrame = null
      if(!HDFSUtil.isParquetFileExist(s"/threshold/data/${mainCode}/dat_order.parquet")){
        ShellUtil.exec(s"hdfs dfs -mkdir /threshold/data/${mainCode}")
        drgDF = df.filter(s"`主干组编码` = '${mainCode}'")

        // 计算平均费用和标准差
        val retList = getDF2List(drgDF.selectExpr("avg(`医疗费用`) `平均费用`","stddev_pop(`医疗费用`) as `标准差`"))
        val minFee:Double = 500.0
        val maxFee:Double = retList.get(0).get("平均费用").toString.toDouble + 3*(retList.get(0).get("标准差").toString.toDouble)

        drgDF = drgDF.where(s"`医疗费用` >= ${minFee} and `医疗费用` <= ${maxFee}")
        drgDF.createOrReplaceTempView(s"${mainCode}_dat_order")
        drgDF= spark.sql(s"select *,left(`主诊断`,5) `ZZD` from ${mainCode}_dat_order")
        drgDF = drgDF.drop("主诊断")
        drgDF = drgDF.withColumnRenamed("ZZD","主诊断")
        drgDF.write.mode(SaveMode.Overwrite).parquet(s"hdfs://master:9000/threshold/data/${mainCode}/dat_order.parquet")
      }else {
        drgDF = spark.read.parquet(s"hdfs://master:9000/threshold/data/${mainCode}/dat_order.parquet")
        drgDF.createOrReplaceTempView(s"${mainCode}_dat_order")
        drgDF= spark.sql(s"select *,left(`主诊断`,5) `ZZD` from ${mainCode}_dat_order")
        drgDF = drgDF.drop("主诊断")
        drgDF = drgDF.withColumnRenamed("ZZD","主诊断")
        drgDF.write.mode(SaveMode.Overwrite).parquet(s"hdfs://master:9000/threshold/data/${mainCode}/dat_order_1.parquet")
        ShellUtil.exec(s"hdfs dfs -rm -r /threshold/data/${mainCode}/dat_order.parquet")
        ShellUtil.exec(s"hdfs dfs -mv /threshold/data/${mainCode}/dat_order_1.parquet /threshold/data/${mainCode}/dat_order.parquet")
      }

      val data:Seq[Any] = Seq(mainCode,drgDF.count().toInt,0,0)
      val row:Row = Row.fromSeq(data)
      rowSeq ++= Seq(row)

      LogUtil.logINFO(s"主干组[${mainCode}]数据写入完毕,还剩[${total - cnt}]个待处理,完成进度为[${(cnt*100.0/total).formatted("%.2f").toString}%]")
      cnt = cnt +1

    })

    val rdd = spark.sparkContext.parallelize(rowSeq)
    val statDF = spark.sqlContext.createDataFrame(rdd, schema)
    statDF.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/threshold/data/threshold_stat.parquet")
    LogUtil.logINFO(s"阈值表计算用所有数据信息统计完毕,写入路径:hdfs://master:9000/threshold/data/threshold_stat.parquet")
  }


  private def init() :Unit = {
    colMaps ++= Map("order_id" -> "病历序号")
    colMaps ++= Map("FEE" -> "医疗费用")
    colMaps ++= Map("ZZD" -> "主诊断")
    colMaps ++= Map("FZD" -> "辅诊断")
    colMaps ++= Map("SS" -> "手术")
    colMaps ++= Map("ADRG" -> "主干组编码")
    colMaps ++= Map("A1" -> "a1")
    colMaps ++= Map("A2" -> "a2")
    colMaps ++= Map("D1" -> "d1")
    colMaps ++= Map("D2" -> "d2")
    colMaps ++= Map("G" -> "g")
    colMaps ++= Map("R" -> "r")
    colMaps ++= Map("S" -> "s")
    colMaps ++= Map("U" -> "u")
  }


  protected  def getDFValueList(sDF:DataFrame,colName: String) :java.util.List[String] = {
    val colLists:java.util.List[String] = new java.util.LinkedList[String]()
    val colDF = sDF.select(s"`$colName`")

    var fieldMap:Map[String,String]  = Map.empty
    colDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    val cols:java.util.LinkedList[java.util.HashMap[String, Object]] = DFUtil.DF2List(colDF,fieldMap)
    for (i<-0 to cols.size()-1){
      val value = cols.get(i).get(s"${colName}").toString
      colLists.add(value)
    }

    colLists
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
