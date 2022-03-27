package com.dahua.utils

object NumFormat {

  def toint(field: String) = {
    try {
      field.toInt
    } catch {
      //不是int类型就抛出异常
      case _: Exception => 0
    }
  }

  def todouble(field: String) = {
    try {
      field.toDouble
    } catch {
      case _: Exception => 0
    }
  }
}
