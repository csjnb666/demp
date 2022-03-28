package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object PcTags extends TagTrait {

  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]
    //设备所在省份名称
    val provincename: String = row.getAs[String]("provincename")
    //设备所在城市名称
    val cityname: String = row.getAs[String]("cityname")
    if (StringUtils.isEmpty(provincename)) {
      map += "ZP" + provincename -> 1
    }
    if (StringUtils.isEmpty(cityname)) {
      map += "ZP" + cityname -> 1
    }
    map
  }
}
