package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 测试kafka实时通道是否打通
 *
 *
 */
object RealtimeStartupApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc: SparkContext = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.Gmall_TOPIC_START, ssc)

    //       startupStream.map(_.value()).foreachRDD{ rdd=>
    //         println(rdd.collect().mkString("\n"))
    //       }

    val startupLogDstream: DStream[StartUpLog] = startupStream.map(_.value()).map { log =>
      // println(s"log = ${log}")
      val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
      startUpLog
    }
  }
}