package org.deep.threshold.util

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path, PathFilter}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * HDFS文件操作工具类
  */
object HDFSUtil {

  private [threshold] var fs : FileSystem = _

  class DeepPathFilter extends PathFilter  {
    override def accept(path: Path): Boolean = true
  }

  /**
    * 获取指定HDFS路径的parquet列表
    * @param path hdfs hdfsURL相对路径
    * @return parquet文件全路径列表
    * example : 获取hdfs://master:9000/demo-6 下的所有parquet文件列表
    *         getParquetFileList("hdfs://master:9000","/demo-6")
    */
  def getParquetFileList(path:String):ListBuffer[String]={
    getFileList(path,"parquet")
  }


  def fsGetOrCreate(): FileSystem = {
    if(fs == null) {
      val url = "hdfs://master:9000/"

      val conf:Configuration = new Configuration()
      conf.set("fs.defaultFS", url)
      conf.setBoolean("dfs.support.append", true)
      fs = FileSystem.get(URI.create(url),conf)
    }
    fs
  }

  /** 判断是否为目录*/
  def isDir(path :Path) : Boolean = {
    fsGetOrCreate.isDirectory(path)
  }

  /** 判断是否为文件*/
  def isFile(path : Path) : Boolean = {
    fsGetOrCreate.isFile(path)
  }


  /**
    * 重命名文件
    * @param oldFilePath
    * @param newFilePath
    * @return
    */
  def reNameFile(oldFilePath:String,newFilePath :String) :Boolean = {
    fsGetOrCreate

    fs.rename(new Path(oldFilePath),new Path(newFilePath))
    true
  }

  /**
    * 删除
    * @param filePath
    * @return
    */
  def deleteFile(filePath:String) :Boolean = {
    fsGetOrCreate

    fs.deleteOnExit(new Path(filePath))
    true
  }

  /**
    * 获取文件列表
    * @param path
    * @param fileType
    * @return
    */
  private def getFileList(path:String, fileType:String):ListBuffer[String]= {
    val hdfs = fsGetOrCreate()
    val filesStatus = hdfs.listStatus(new Path(path), new DeepPathFilter)
    val list:ListBuffer[String] = new ListBuffer[String]

    for(status <- filesStatus){
      val filePath : Path = status.getPath
      val fileName = new Path(filePath.toString).getName

      if(isDir(filePath) && fileName.endsWith(s".${fileType}")) list += filePath.toString
    }
    list
  }

  /**
    * 判断指定路径的parquet文件是否存在
    * @param path parquet文件路径(hdfsURL相对路径)
    * @return true-存在,false-不存在
    */
  def isParquetFileExist(path:String) : Boolean ={
    isHDFSFileExist(path,"parquet")
  }

  /**
    * 判断指定路径的ini文件是否存在
    * @param path parquet文件路径(hdfsURL相对路径)
    * @return true-存在,false-不存在
    */
  def isIniFileExist(path:String) : Boolean ={
    isHDFSFileExist(path,"ini")
  }

  /**
    *  判断指定路径的HDFS文件是否存在
    * @param path     hdfs 文件路径
    * @param fileType 文件类型
    * @return true-存在,false-不存在
    */
  private def isHDFSFileExist( path:String, fileType:String) :Boolean = {

    if(fileType.equals("csv") || fileType.equals("parquet") || fileType.equals("ini")){
      if(isDir(new Path(path)) && path.endsWith(s".${fileType}") && isFile(new Path(s"${path}/_SUCCESS")))  return true
    }else{
      if(isFile(new Path(path)) && path.endsWith(s".${fileType}"))  return true
    }

    false
  }


  /**
    * 以覆盖的方式将数据写入HDFS文件中
    * @param hdfsURL hdfs URL地址
    * @param path    hdfs 文件路径
    * @param value   数据内容
    * @return
    */
  def saveContent2File(hdfsURL:String, path:String,value:String) :Boolean = {
    val fos:FSDataOutputStream = fsGetOrCreate().create(new Path(hdfsURL+path ),true)
    fos.write(value.getBytes("UTF-8"))
    fos.flush()
    fos.close()

    true
  }

  /**
    * parquet 文件是否正在被追加
    * @param path 文件路径
    * @return true-是,false-否
    */
  def isParquetFileTemp(path:String) :Boolean = {
    isDir(new Path(s"${path}/_temporary"))
  }

//  private def getConfigMap(spark:SparkSession, filePath:String):Map[String,Any] = {
//
//    val jsonParse = new DAPJSONParse
//
//    var jsonStr:String = ""
//    spark.read.textFile(filePath).collect().foreach(line => {
//      jsonStr = jsonStr + line.replaceAll("\t"," ")
//    })
//
//    val jsonObject:Map[String,Any] = jsonParse.parseJSON(jsonStr)
//    jsonObject
//  }

}
