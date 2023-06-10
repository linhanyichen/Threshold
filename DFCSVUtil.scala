package org.deep.threshold.util
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object DFCSVUtil {

  /**
    * 将DataFrame保存至CSV文件中
    * @param df 待保存的结果集
    * @param filePath hdfs路径
    * @param saveMode 保存模式
    * @param header 是否写入文件头(即列名称)
    */
  def saveDF2CSV(df:DataFrame, filePath:String, saveMode:SaveMode = SaveMode.Overwrite,header:Boolean = true) :Unit = {
    try {
      df.write
        .mode(saveMode)
        .format("com.databricks.spark.csv")
        .option("header", s"${header}") // true - csv文件写入列名,false -不写入列名
        .save(filePath)
    }catch{
      case e:Exception => e.printStackTrace()
    }
  }

  /**
    * 读取CSV文件,并将其转换为DataFrame
    * @param spark SparkSession 对象
    * @param filePath 文件路径(全路径)
    * @param header  是否读取文件头
    * @return
    */
  def readCSVAsDF(spark:SparkSession, filePath :String ,header:Boolean = true) :DataFrame = {
    var df:DataFrame = null
    try{
      val sqlContext = spark.sqlContext
      df = sqlContext.load("com.databricks.spark.csv", Map("path" -> filePath, "header" -> s"${header}"))
    }catch{
      case e:Exception => e.printStackTrace()
    }

    return df
  }
}
