package org.deep.threshold.util

import org.apache.spark.sql.DataFrame

object DFExcelUtil {

  /**
    * 将DataFrame保存至
    * @param df 待保存的结果集
    * @param filePath hdfs路径
    * @param sheetName Excel sheet名称
    * @param needHead 是否需要表头
    */
  def saveDF2Excel(df:DataFrame, filePath:String,sheetName:String="Sheet1",needHead:Boolean = true) :Unit = {
    try{
      df.write
        .format("com.crealytics.spark.excel")
        .option("sheetName", sheetName)
        .option("useHeader", needHead)
        .option("dateFormat", "yy-mm-dd")
        .option("timestampFormat", "yyyy-mm-dd hh:mm:ss.000")
        .mode("overwrite")
        .save(filePath)
    }catch{
      case e:Exception => null
    }
  }

}
