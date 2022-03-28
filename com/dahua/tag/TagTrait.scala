package com.dahua.tag

trait TagTrait {

  /**
   * 特性
   * 所有数据都能转换为map集合
   */
  def makeTags(args: Any*): Map[String, Int]
}
