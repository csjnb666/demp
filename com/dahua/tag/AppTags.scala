package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AppTags extends TagTrait {

  override def makeTags(args: Any*): Map[String, Int] = {

    //设定返回值类型
    var map = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]

    //接收广播变量的值
    val stringToString: Map[String, String] = args(1).asInstanceOf[Map[String, String]]
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")

    //渠道标签
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    if (StringUtils.isEmpty(appname)) {
      stringToString.contains("appid") match {
        case true => map + "APP" + stringToString.getOrElse("appid", "未知") -> 1
      }
    } else {
      map + "APP" + appname -> 1
    }

    //渠道标签
    map + "CN" + adplatformproviderid -> 1
    map
  }
}
