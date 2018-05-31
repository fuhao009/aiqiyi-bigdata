package com.study.spark.project

import com.study.spark.project.Utils.DateuUtils
import com.study.spark.demo._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object StatStraemingAPP {
  // 必须设置,local[2]表示两颗内核,StatStreamingApp可以自定义
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "StatStreamingApp", Seconds(5))

    val kafkaParams = Map[String, Object](
      // 参数说明参考 http://kafka.apache.org/documentation.html#newconsumerconfigs
      // 有几台kafka就写几台,如"192.168.16.192:9092,192.168.16.193:9092,…,"
      "bootstrap.servers" -> "slave1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // createDirectStream的组ID
      "group.id" -> "test",
      // 自动
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // flume中的a1.sinks.k1.topic
    val topics = Array("flumeTopic")
    val logs = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value())
    var cleanLog = logs.map(line=>{
      var infos = line.split("\t")
      var ip = infos(0)
      var date_time = DateuUtils.parseToMin(infos(1))
      var url = infos(2).split(" ")(1)
      var categaryId = 0
      // 访问来源
      var refer = infos(3)
      // 状态码
      var statuscode = infos(4).toInt
      // 如果请求的不是www开头的,则categaryID = 0
      if(url.startsWith("www")){
        categaryId = url.split("/")(1).toInt
      }
      // 返回一个calss,方便filter中调用class属性来过滤
      ClickLog(ip,date_time,url,categaryId,refer,statuscode)
    }).filter(log => log.categoryId!=0)
    // 输出
    // cleanLog.print()
    // 功能一：每个类别的点击量保存到HBase中,(day_categaryID,1)
    val rdd1 = cleanLog.map(log =>
      (log.time.substring(0,8)+log.categoryId,1)
      // 相同key计数,再迭代
    )
    // rdd1.print()
    val rdd2 = rdd1.reduceByKey(_ + _).foreachRDD( rdd =>{
        // 分区
        rdd.foreachPartition( partition =>{
          // 因为save方法需要传入一个list，所以要将每个class转list,用于保存(day_categaryID,1)数据
          val list = new ListBuffer[CategoryClickCount]
          partition.foreach( pair=>{
            // 将(day_categaryID,1)数据添加到列表中
            print(list)
            list.append(CategoryClickCount(pair._1,pair._2.toInt))
          })
          // 保存到HBase中
          CategaryClickCountDAO.save(list)
        })
      }
    )
    //  功能二：针对不同的渠道和类别进行分析
    // val和var区别：val不可修改值,var可以修改值
    val rdd3 = cleanLog.map(log=>{
      val url = log.refer.replace("//","/")
      val splits = url.split("/")
      var host = ""
      if(url.length >= 2){
        host = splits(1)
      }
      (host, log.time.substring(0,8), log.categoryId)
    })
    // 判断host不为空
    val rdd4 = rdd3.filter(x => x._1 != "").map( log =>{
      // (time_来源_栏目ID,1)
      (log._2 + "_" + log._1 + "_" + log._3,1)
    })
//    rdd4.print()
    // 迭代 ---> 分区1 -->  创建ListBuffer --> 迭代 --> 保存到HBase
    //           分区2 -->  创建ListBuffer --> 迭代 --> 保存到HBase
    // 聚合
    val rdd5 = rdd4.reduceByKey(_+_)
    // 迭代
    val rdd6 = rdd5.foreachRDD(rdd=>{
      // 分区
      rdd.foreachPartition(partition=>{
        // ListBuffer是可变的,元素是从右添加,并且可以保存元素
        // list 是从左添加,不可变
        // ListBuffer[CategorySearchClickCount] 表示创建一个能存储CategorySearchClickCount类格式的列表
        val list = new ListBuffer[CategorySearchClickCount]
          // 每个区迭代
          partition.foreach(paris=>{
            // 返回ListBuffer(CategoryClickCount(201805296,11), CategoryClickCount(201805294,12))
            // 两种写法
            // 1：list += CategorySearchClickCount(paris._1,paris._2)
            // 2：list.append(CategorySearchClickCount(paris._1,paris._2))
          list.append(CategorySearchClickCount(paris._1,paris._2))
//            print(list)
        })
        // 保存数据
        CategarySerachClickCountDAO.save(list)
      })
    })
    // 启动
    ssc.start()
    // 持续运行
    ssc.awaitTermination()
  }

}
