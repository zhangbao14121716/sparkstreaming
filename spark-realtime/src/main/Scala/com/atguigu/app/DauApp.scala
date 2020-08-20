package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.Dauhandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Zhangbao
 * @Date: 15:33 2020/8/15
 * @Description:
 *
 *
 * DAGscheduler : 划分阶段和任务
 * TASKsheduler : 排序, FIFO任务执行顺序
 * SchedulerBackend : 提交任务
 */
object DauApp {


  def main(args: Array[String]): Unit = {

    //1.创建配置对象
    val conf: SparkConf = new SparkConf().setAppName("gmallreal").setMaster("local[*]")
    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)
    //3.创建sparkStreaming
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    //4.用工具类中的方法获取kafka数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.Gmall_TOPIC_START, ssc)
    //5.为数据流按照样例类（case class）转换结构，补充上时间字段
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //record的类型ConsumerRecord<K,V>
      val str: String = record.value()
      //将字符串转换为样例类形式的json数据
      val startUpLog: StartUpLog = JSON.parseObject(str, classOf[StartUpLog])
      //将转换后的json的key为ts的时间戳转换成date，hour
      val dateString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startUpLog.ts))
      //日期时间切分数组
      val dateStrings: Array[String] = dateString.split(" ")
      //对json中空值补充
      startUpLog.logDate = dateStrings(0)
      startUpLog.logHour = dateStrings(1)
      //返回start日志的json数据流
      startUpLog
    })
    //6.根据Redis中保存的数据进行跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = Dauhandler.filterByRedis(startLogDStream, sc)
    //7.对第一次去重后的数据进行同批次去重
    val filterByGroupDStream: DStream[StartUpLog] = Dauhandler.filterByGroup(filterByRedisDStream, sc)
    filterByGroupDStream.cache()
    //8.将两次去重后的startLog的mid写入Redis
    Dauhandler.saveMidToRedis(filterByGroupDStream)
    //9.将两次去重后的数据明细写入Hbase数据库
    Dauhandler.saveStartLogToHBase(filterByGroupDStream,"GMALL200317_DAU")
    //10.执行任务,---spark任务内核提交入口
    ssc.start()
    ssc.awaitTermination()
  }

}
