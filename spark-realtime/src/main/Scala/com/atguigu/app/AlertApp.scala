package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyESUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._
/**
 * @Author: Zhangbao
 * @Date: 18:18 2020/8/18
 * @Description:
 *
 */
object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSonf，StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyAlertApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //2.kafka工具类创建获取kafka数据流，获取事件主题的kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.Gmall_TOPIC_EVENT, ssc)
    //3.转化结构
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {
      val eventLogs: String = record.value()
      //a.转化成Json对象
      val eventLog: EventLog = JSON.parseObject(eventLogs, classOf[EventLog])
      //b.处理日期小时
      val dateToHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(eventLog.ts))
      eventLog.logDate = dateToHourString.split(" ")(0)
      eventLog.logHour = dateToHourString.split(" ")(1)
      //c.返回值
      eventLog
    })
    //4.开窗
    val windowDStream: DStream[EventLog] = eventLogDStream.window(Seconds(30), Seconds(5))
    //5.分组
    val groupByMidDStream: DStream[(String, Iterable[EventLog])] = windowDStream.map(eventLog => {
      (eventLog.mid, eventLog)
    }).groupByKey()
    //6.判断是否预警
    val couponDStream: DStream[CouponAlertInfo] = groupByMidDStream.map { case (mid, eventLogIter) => {
      //6.1对每个不同mid进行map映射
      var noClick: Boolean = true
      val uidsSet: util.HashSet[String] = new java.util.HashSet[String]
      val itemIdsSet: util.HashSet[String] = new java.util.HashSet[String]
      val eventsList: util.ArrayList[String] = new util.ArrayList[String]()
      breakable(
        //6.2循环遍历迭代的每条数据
        for (eventLog: EventLog <- eventLogIter) {
          //6.3每条行为写入行为集合
          eventsList.add(eventLog.evid)
          if (eventLog.evid.equals("coupon")) {
            //6.4满足领用优惠券的用户写入Set集合
            uidsSet.add(eventLog.uid)
            //6.5将商品信息也写入集合
            itemIdsSet.add(eventLog.itemid)
          } else if (eventLog.evid.equals("clickItem")) {
            //6.6当前批次有点击行为的，将非点击行为置为false，并终止循环
            noClick = false
            break()
          }
        })
        // 对每个mid，判断uid大于3 并且 没有点击行为
        if (uidsSet.size() >= 3 && noClick) {
          //返回预警日志
          CouponAlertInfo(mid, uidsSet, itemIdsSet, eventsList, System.currentTimeMillis())
        } else {
          null
        }
      }
    }
    //7.过滤空的日志
    val filterDStream: DStream[CouponAlertInfo] = couponDStream.filter(couponDStream => {
      couponDStream != null
    })
    filterDStream.cache()
    filterDStream.print(100)
    //8.预警日志写入ES
    filterDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //预警信息结构转换
        val dataList: List[(String, CouponAlertInfo)] = iter.map(couponInfo => {
          val minutes: Long = couponInfo.ts / 1000 / 60
          (s"${couponInfo.mid}-$minutes", couponInfo)
        }).toList
        //
        val date: String = LocalDate.now().toString
        MyESUtil.insertByBulk(GmallConstants.GMALL_ES_ALERT_PRE + "_" + date, "_doc", dataList)
      })
    })
    //9.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
