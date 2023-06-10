package org.deep.threshold.util

import org.json4s.jackson.JsonMethods.parse

class JSONUtil {

  private[this] var jsonMap :Map[String,Any] = Map.empty[String,Any]

  /**
    * json解析类
    * @param jsonStr，符合json格式的字符串
    * @return
    */
  def parseJSON(jsonStr:String) :Map[String,Any] = {
    jsonMap=parse(jsonStr).values.asInstanceOf[Map[String,_]]
    jsonMap
  }


  /**
    * 获取jsonMap 数据信息
    */
  def getDataMap() :Map[String,Any] = {
    jsonMap
  }

  /**
    * 遍历当前json对象Map,获取给定节点值
    * @param keys json key值,多级时用.分割，例如:a.b.c
    * @return value值,null-未找到key对应值
    */
  def getValue(keys:String) :String = {
    val keyList = keys.split('.').toList
    var ret :Any = null
    var map:Map[String,Any] = jsonMap

    for (key <- keyList){
      if(!map.keySet.contains(key))  return null   // 如果无此节点,则返回NULL
      ret = map.get(key).get

      if(!key.equals(keyList.last)){
        // value值为Map类型数据
        if(ret.isInstanceOf[Map[String,Any]]){
          map = ret.asInstanceOf[Map[String,Any]]
        }// value值为List[Map]类型数据
        else if(ret.isInstanceOf[List[Map[String,Any]]]){
          map = ret.asInstanceOf[List[Map[String,Any]]](0)
        }// 无此节点
        else{
          return null
        }
      }
    }

    ret.asInstanceOf[String]
  }

  /**
    * 遍历当前json对象Map,获取给定节点List数组信息
    * @param keys json key值,多级时用.分割，例如:a.b.c
    * @return vList数组,null-未找到key对应值
    */
  def getListValue(keys:String) :List[Map[String,Any]] = {
    val keyList = keys.split('.').toList
    var ret :Any = null
    var map:Map[String,Any] = jsonMap

    for (key <- keyList){
      if(!map.keySet.contains(key))  return null
      ret = map.get(key).get

      if(!key.equals(keyList.last)){
        // value值为Map类型数据
        if(ret.isInstanceOf[Map[String,Any]]){
          map = ret.asInstanceOf[Map[String,Any]]
        }// value值为List[Map]类型数据
        else if(ret.isInstanceOf[List[Map[String,Any]]]){
          map = ret.asInstanceOf[List[Map[String,Any]]](0)
        }// 无此节点
        else{
          return null
        }
      }
    }

    ret.asInstanceOf[List[Map[String,Any]]]
  }


  /**
    * 遍历当前json对象Map,获取给定节点List数组信息
    * @param keys json key值,多级时用.分割，例如:a.b.c
    * @return vList数组,null-未找到key对应值
    */
  def getMapValue(keys:String) :Map[String,Any] = {
    val keyList = keys.split('.').toList
    var ret :Any = null
    var map:Map[String,Any] = jsonMap

    for (key <- keyList){
      if(!map.keySet.contains(key))  return null
      ret = map.get(key).get

      if(!key.equals(keyList.last)){
        // value值为Map类型数据
        if(ret.isInstanceOf[Map[String,Any]]){
          map = ret.asInstanceOf[Map[String,Any]]
        }// value值为List[Map]类型数据
        else if(ret.isInstanceOf[List[Map[String,Any]]]){
          map = ret.asInstanceOf[List[Map[String,Any]]](0)
        }// 无此节点
        else{
          return null
        }
      }
    }

    ret.asInstanceOf[Map[String,Any]]
  }


}
