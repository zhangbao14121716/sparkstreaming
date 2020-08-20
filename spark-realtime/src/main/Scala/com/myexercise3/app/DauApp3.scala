package com.myexercise3.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.constants.GmallConstants
import com.myexercise3.bean.StartLogUp3
import com.myexercise3.handler.DauHandler3
import com.myexercise3.utils.MyKafkaUtil3
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Duration, Milliseconds, Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Zhangbao
 * @Date: 19:50 2020/8/17
 * @Description:
 *
 */
object DauApp3 {

  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    //2.创建SparContext对象
    val sc: SparkContext = new SparkContext(sparkConf)
    //3.创建StreamingContext程序入口
//    val duration: Duration = Seconds(5)
//    val duration1: Duration = Minutes(5)
//    val duration2: Duration = Milliseconds(1000)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    //4.获取用工具类获取kafka数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil3.getKafkaDStream(GmallConstants.Gmall_TOPIC_START, ssc)
    //5.对获取到的数据流进行结构转换
    val startLogUpDStream: DStream[StartLogUp3] = kafkaDStream.map(record => {
      //获取json格式的字符串
      val startLog: String = record.value()
      //将字符串转换成Json对象
      val startLogUp: StartLogUp3 = JSON.parseObject(startLog, classOf[StartLogUp3])
      //对ts进行格式转换
      val dateToHour: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startLogUp.ts))
      //对json对象中的空值补充
      startLogUp.logDate = dateToHour.split(" ")(0)
      startLogUp.logHour = dateToHour.split(" ")(1)
      startLogUp
    })
    //6.对数据进行跨批次去重，力度较大
    val filterByRedis: DStream[StartLogUp3] = DauHandler3.filterByRedis(startLogUpDStream, sc)
    //7.对数据进行同批次去重，力度较小，做缓存处理
    val filterByGroup: DStream[StartLogUp3] = DauHandler3.filterByGroup(filterByRedis, sc)
    filterByGroup.cache()
    //8.数据写入redis
    DauHandler3.saveToRedis(startLogUpDStream)
    //9.数据写入Hbase
    DauHandler3.saveToHBase(filterByGroup,"GMALL200317_DAU")
    //10.执行任务
    ssc.start()
    ssc.awaitTermination()
  }
}
