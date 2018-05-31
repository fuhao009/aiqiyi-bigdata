package com.study.spark.demo

/**
  * 类栏目点击次数统计字段
  * case class 可以省略new
  * @param day_categoryId
  * @param clickCount // HBase只能识别String 其他都会变成不可读如：value=\x00\x00\x00\x00\x00\x00\x00\x18
  */
case class CategoryClickCount(day_categoryId:String,clickCount:Int)