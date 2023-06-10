package org.deep.threshold.dict

/**
  * 数据字典及常量定义类
  * create by zhuy 2019-10-29
  */
object Dict {

  final val HDFSURL:String = "hdfs://master:9000"

  /************各主干组数据存放路径信息************/
  // 主干组数据路径
  final val baseDataPath:String = s"${HDFSURL}/threshold/data/&maincode/dat_order.parquet"
  // 子集费用分析表数据路径
  final val subFeeDataPath:String = s"${HDFSURL}/threshold/data/base/&maincode/sub_fee_data.parquet"
  // 计算产生的数据存储位置
  final val setDataPath:String = s"${HDFSURL}/threshold/data/base/&maincode/"


  /***************亚组编码定义***************/
  final val DRG_SUBCODE_5:String = "5"    // 轻微合并症及伴随病(XXX5)
  final val DRG_SUBCODE_3:String = "3"    // 一般性合并症及伴随病(XXX3)
  final val DRG_SUBCODE_2:String = "2"    // 较重合并症及伴随病(XXX2)
  final val DRG_SUBCODE_1:String = "1"    // 重度合并症及伴随病(XXX1)
  final val DRG_SUBCODE_9:String = "9"    // 不能细分的为XXX9

}
