package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
/**
 * @Author: Zhangbao
 * @Date: 15:44 2020/8/18
 * @Description:
 *
 */
object OrderApp {
  def main(args: Array[String]): Unit = {

    //1.创建配置对象
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    //2.创建SparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    //3.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    //4.kafka工具类获取kafka数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER, ssc)
    //5.对数据的结构转换
    //"payment_way":"2","delivery_address":"zVgUdJkWczoUHBNjIMLv","consignee":"dQTXeJ","create_time":"2020-08-17 04:07:51"
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val orderData: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(orderData, classOf[OrderInfo])
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      //电话，收件人脱敏
      orderInfo.consignee_tel = orderInfo.consignee_tel.take(3) + "****" + orderInfo.consignee_tel.takeRight(4)
      orderInfo
    })

    orderInfoDStream.cache()
    orderInfoDStream.print()
    //6.将数据写入到HBase
    orderInfoDStream.foreachRDD(rdd => {
      val cols: Array[String] = classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase)
      rdd.saveToPhoenix("GMALL0317_ORDER_INFO",
        cols,
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })
    //7.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
