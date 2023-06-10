package org.deep.threshold.assist

import java.sql.{Connection, DriverManager}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.deep.threshold.util.{DFUtil, HDFSUtil, LogUtil}

/**
  * 获取所有纳入阈值表计算的样本数据
  * create by zhuy 2021-04-02
  */
object GetAllValidSampleData {

  var spark:SparkSession = null
  var codeList:List[String] = List.empty

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder().appName("GetAllValidSampleData").getOrCreate()

    try {

      getAllCodeList()
      dealSampleData()

    }catch {
      case e:Exception => e.printStackTrace()
    }

  }


  private def getAllCodeList():Unit = {

    var df = spark.read.parquet("hdfs://master:9000/threshold/data/threshold_stat.parquet")
    df.createOrReplaceTempView("threshold_stat")
    df = spark.sql("select * from threshold_stat order by `主干组编码` asc")
    val dataList = getDF2List(df)

    dataList.toArray.foreach(c => {
      val map:java.util.HashMap[String,Any] = new java.util.HashMap[String,Any]()
      val col = c.asInstanceOf[java.util.HashMap[String,Object]]
      codeList ++= List(col.get("主干组编码").toString)
    })

    LogUtil.logINFO(s"主干组编码收集完毕,共需处理[${codeList.size}]个主干组")
    println(codeList)
  }

  private def dealSampleData() :Unit = {
    var dfAll:DataFrame = null
    var dfSubset:DataFrame = null
    var dfRet:DataFrame = null
    val size:Int = codeList.size
    var iCnt:Int = 0

    codeList.foreach(code => {
      if (HDFSUtil.isParquetFileExist(s"hdfs://master:9000/threshold/data/base/${code}/${code}_all.parquet")){
        dfAll = spark.read.parquet(s"hdfs://master:9000/threshold/data/base/${code}/${code}_all.parquet")
        dfSubset = spark.read.parquet(s"hdfs://master:9000/threshold/data/base/${code}/sub_fee_data.parquet")

        dfAll.createOrReplaceTempView(s"${code}_all")
        dfSubset.createOrReplaceTempView(s"${code}_sub_fee_data")

        dfRet = spark.sql(s"select s.`DRG组编码` drg_code,t.`病历序号` order_id,t.`主干组编码` adrg,t.`医疗费用` fee,t.`子集码` sub_code from ${code}_all t left join ${code}_sub_fee_data s on t.`子集码` = s.`子集码`")
      }else{
        dfRet = spark.read.parquet(s"hdfs://master:9000/threshold/data/${code}/dat_order.parquet")
        dfRet.createOrReplaceTempView(s"${code}_dat_order")
        dfRet = spark.sql(s"select '${code}9' as drg_code,`病历序号` order_id,`主干组编码` adrg,`医疗费用` fee,'' as sub_code from ${code}_dat_order")
      }

      // 将参与计算的阈值表结果数据写入数据库
      val url = "jdbc:mysql://192.168.0.100:3306/data_prepare?characterEncoding=UTF-8"
      val table="threshold_sample"
      val prop = new java.util.Properties
      prop.setProperty("user","root")
      prop.setProperty("password","root")
      dfRet.write.mode(SaveMode.Append).jdbc(url,table,prop)
      iCnt = iCnt + 1

      LogUtil.logINFO(s"[主干组 -> ${code}]已完成[${(iCnt*100.0/size).formatted("%.2f").toString}%]的主干组回写操作处理")
    })
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
