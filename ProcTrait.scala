package org.deep.threshold.memproc

import java.util

import org.apache.spark.sql.DataFrame
import org.deep.threshold.util.DFUtil

trait ProcTrait {

  protected var mainCode:String = ""
  protected var df:DataFrame = null

  protected var subFeeDataFilePath :String = ""
  protected var dataSavePath:String = ""
  protected var subGroupFeeDF:DataFrame = null            // 子集费用分析DF
  /**
    * 获取结果集字段列表信息
    * @param sDF
    * @param colName
    * @return
    */
  protected  def getDFValueList(sDF:DataFrame,colName: String) :util.List[String] = {
    val colLists:util.List[String] = new util.LinkedList[String]()
    val colDF = sDF.select(s"`$colName`")

    var fieldMap:Map[String,String]  = Map.empty
    colDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    val cols:util.LinkedList[util.HashMap[String, Object]] = DFUtil.DF2List(colDF,fieldMap)
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
  protected def getDF2List(sDF:DataFrame) :util.List[util.HashMap[String,Object]] = {
    var fieldMap:Map[String,String]  = Map.empty
    sDF.schema.foreach(field => {
      val dataType = field.dataType.toString.replaceAll("Type","")
      fieldMap ++= Map(field.name -> dataType)
    })

    DFUtil.DF2List(sDF,fieldMap)
  }

  /**
    * 将JAVA的List转换为Scala的List
    * @param list
    * @return
    */
  protected def List2List(list:util.List[String]) :List[String] = {
    var sList:List[String] = List.empty
    list.toArray.foreach(value => sList ++= List(value.toString))

    sList
  }

}
