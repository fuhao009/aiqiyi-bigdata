package com.study.spark.demo

import com.study.spark.project.Utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * tableName 表名
  * cf 列族
  * quilifer 统计的词
  */
object CategaryClickCountDAO {
  val tableName = "category_clickcount"
  val cf = "info"
  val qualifer = "click_count"
  // 保存数据到HBase
  // CategoryClickCount包含了多个参数,转list
  def save(list:ListBuffer[CategoryClickCount]): Unit ={
    val table = HBaseUtils.getInstance().getHtable(tableName)
    // 迭代写入数据到HBASE
    for(els <- list){
      // rowid, family,qualifier,amount
      // 如果rowid相同,则会将value进行相加
      table.incrementColumnValue(Bytes.toBytes(els.day_categoryId),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCount)
    }
  }
  // 根据rowid查询值
  def count(day_categary:String): Long ={
    // 获取表实例
    val table = HBaseUtils.getInstance().getHtable(tableName)
    // 获取查询器实例
    val get = new Get(Bytes.toBytes(day_categary))
    // 通过上面实例获取到值
    val value = table.get(get).getValue(cf.getBytes(),qualifer.getBytes())
    // 判断是否是空表
      if(value == null){
        // 返回空长整数
        0L
      }else{
        // 返回查询结果
        Bytes.toLong(value)
      }
  }
  // 执行写入
  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CategoryClickCount]
    /**写入数据
      * rowid
      * click_num
      */
    list.append(CategoryClickCount("20171122_11",300))
    list.append(CategoryClickCount("20171122_12",300))
    save(list)
    print(count("20171122_12") + "--" + count("20171122_12"))

  }
}


