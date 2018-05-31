package com.study.spark.demo

/**
  * 获取日志字段
  * @param ip
  * @param time
  * @param categoryId
  * @param refer
  * @param statuscode
  */
case class ClickLog(ip:String,time:String,url:String,categoryId:Int,refer:String,statuscode:Int)