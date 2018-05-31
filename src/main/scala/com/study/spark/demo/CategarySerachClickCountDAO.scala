package com.study.spark.demo

import com.study.spark.project.Utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
// 根据不同的分词完成业务
/**
  * tableName 表名
  * cf 列族
  * quilifer 统计的词
  */
object CategarySerachClickCountDAO {
  val tableName = "category_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"
  // 保存数据到HBase
  // CategoryClickCount包含了多个参数,转list
  def save(list:ListBuffer[CategorySearchClickCount]): Unit ={
    val table = HBaseUtils.getInstance().getHtable(tableName)
    // 迭代写入数据到HBASE
    for(els <- list){
      // rowid, family,qualifier,amount
      // 如果rowid相同,则会将value进行相加
      table.incrementColumnValue(Bytes.toBytes(els.day_search_categoryId),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCount)
    }
  }
  // 根据rowid查询值
  def count(day_search_categoryId:String): Long ={
    // 获取表实例
    val table = HBaseUtils.getInstance().getHtable(tableName)
    // 获取查询器实例
    val get = new Get(Bytes.toBytes(day_search_categoryId))
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
    val list = new ListBuffer[CategorySearchClickCount]
    /**写入数据
      * rowid 20171122_1(渠道)_1()
      * click_num
      */
    list.append(CategorySearchClickCount("20171122_1_1",300))
    list.append(CategorySearchClickCount("20171122_1_2",300))
    save(list)
    print(count("20171122_1_2") + "--" + count("20171122_1_2"))

  }
}


