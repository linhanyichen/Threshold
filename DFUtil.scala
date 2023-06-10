package org.deep.threshold.util

import java.util

import org.apache.spark.sql.{DataFrame, Row}

/**
  * DataFrame 工具类
  * create by zhuy 2019-10-29
  */
object DFUtil {

  /**
    * DataFrame转化成List
    * @param df DataFrame结果集
    * @return 结果集list
    */
  def DF2List(df: DataFrame,fieldDataType:Map[String,String]): util.LinkedList[util.HashMap[String, Object]] = {
    val retRows = df.collect()
    Array2List(retRows,fieldDataType)
  }

  /**
    * Array[Row]数组转化成List
    *
    * @param retRows Array[Row]结果集
    * @param fieldDataType 结果集字段名与类型Map
    * @return list
    */
  def Array2List(retRows: Array[Row],fieldDataType:Map[String,String]): util.LinkedList[util.HashMap[String, Object]] = {
    val resList = new util.LinkedList[util.HashMap[String, Object]]()

    // df为null时.直接返回null
    if (retRows.isEmpty) return resList

    retRows.foreach(row => {
      val map = new util.HashMap[String, Object]()
      row.schema.fields.foreach(st => {
        genFieldMap(st.name,row.getAs(st.name),fieldDataType,map)
      })
      resList.add(map)
    })

    resList
  }

  /**
    * 生成结果集中的各字段k-v Map
    * @param fieldName       字段名
    * @param fieldValue      字段值
    * @param fieldDataType   字段数值类型Map
    * @return 字段k-v Map
    */
  private def genFieldMap(fieldName:String,fieldValue:Object,fieldDataType:Map[String,String],fieldMap:util.HashMap[String,Object]) :Unit = {
    val dataType = fieldDataType.get(fieldName).get

    // 浮点型
    if (dataType.equals("Double") || dataType.equals("Float")) {
      val nullV:Double = 0.0
      if (fieldValue == null){
        fieldMap.put(fieldName, nullV.asInstanceOf[Object])
      }
      else if (fieldValue.equals(Double.NaN) || fieldValue.equals(Float.NaN)) {
        fieldMap.put(fieldName, nullV.asInstanceOf[Object])
      } else {
        val valueDouble: Double = if (dataType.equals("Float")) fieldValue.asInstanceOf[Float].toDouble else  fieldValue.asInstanceOf[Double]
        // 保留九位小数
        fieldMap.put(fieldName, convertDouble(f"$valueDouble%20.9f".trim).asInstanceOf[Object])
      }
    }else if (dataType.equals("Integer")) {
      val nullV :Int =0
      if (fieldValue == null) fieldMap.put(fieldName, nullV.asInstanceOf[Object])
      else fieldMap.put(fieldName, fieldValue.asInstanceOf[Int].asInstanceOf[Object])
    }
    else if (dataType.equals("Long")) {
      val nullV :Long =0
      if (fieldValue == null) fieldMap.put(fieldName, nullV.asInstanceOf[Object])
      else fieldMap.put(fieldName, fieldValue.asInstanceOf[Long].asInstanceOf[Object])
    }
    else{
      if (fieldValue == null) fieldMap.put(fieldName, "".asInstanceOf[Object])
      else                    fieldMap.put(fieldName, fieldValue)
    }
  }

  /**
    * 格式化去掉Double类型数据尾部多余的0
    * @param str Double串
    * @return 去掉尾部0之后的Double数值
    */
  private def convertDouble(str:String) :Double = {
    var doubleStr:String = str
    // 去掉格式化后末尾多余的0
    if(doubleStr.contains(".")){
      while(doubleStr.endsWith("0")){
        doubleStr = doubleStr.substring(0,doubleStr.length-1)
      }

      if(doubleStr.endsWith(".")) doubleStr = doubleStr.substring(0,doubleStr.length-1)
    }
    doubleStr.toDouble
  }
}
