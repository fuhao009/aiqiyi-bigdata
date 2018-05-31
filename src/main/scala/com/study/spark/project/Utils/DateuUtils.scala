package com.study.spark.project.Utils
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

// 把当前时间转成时间戳
object DateuUtils {
  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TAG_FORMAT = FastDateFormat.getInstance("yyyyMMdd")

  // 获取时间
  def getTime(time:String):Long = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }
  def parseToMin(time:String)={
    TAG_FORMAT.format(new Date(getTime(time)))
}
  def main(args: Array[String]): Unit = {
    println(getTime("2018-05-27 10:49:00"))
  }
}
