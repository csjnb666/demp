package com.dahua.tag

import org.apache.spark.sql.Row

object KeyTags extends TagTrait {

  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]

    //停用词
    val stringToInt: Map[String, Int] = args(1).asInstanceOf[Map[String, Int]]
    //关键字
    val keywords: String = row.getAs[String]("keywords")
    //关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
    val ks: Array[String] = keywords.split("\\|")
    //关键字个数不能少于 3 个字符，且不能超过 8 个字符
    ks.filter(x => x.length >= 3 && x.length <= 8 && !stringToInt.contains(x))
      .foreach(x => map += "K" + x -> 1)
    map
  }
}
