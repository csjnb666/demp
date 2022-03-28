package com.dahua.tag

import com.dahua.tools.SNTools
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object SQTags extends TagTrait {

  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]
    //设备所在经度
    val longrow: String = row.getAs[String]("long")
    //设备所在纬度
    val latrow: String = row.getAs[String]("lat")

    //如果经度不为空 纬度不为空
    if (StringUtils.isNotEmpty(longrow) && StringUtils.isNotEmpty(latrow))
      if (latrow.toDouble>3&&latrow.toDouble<54 && longrow.toDouble>73 &&longrow.toDouble<136){
        //调用方法 放入经纬度求值
        val str: String = SNTools.getBusiness(latrow + "," + longrow)
        map += "SQ" + str -> 1
      }
    map
  }
}
