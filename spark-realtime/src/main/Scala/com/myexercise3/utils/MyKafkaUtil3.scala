package com.myexercise3.utils

import java.{io, lang}
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @Author: Zhangbao
 * @Date: 0:44 2020/8/17
 * @Description:
 *
 */
object MyKafkaUtil3 {

  //1.创建配置信息对象
  private val properties: Properties = PropertiesUtil3.load("config.properties")
  //2.用于初始化连接到集群地址
  private val brokers: String = properties.getProperty("kafka.broker.list")
  //3.kafka消费者配置

  private val group: String = "bigdata0317"
  private val deserializathion: String = "org.apache.kafka.common.serialization.StringDeserializer"
  //将kafka配置放入Map集合中
  private val kafkaParams = Map(
    //配置消费者组
    ConsumerConfig.GROUP_ID_CONFIG -> group,
    //配置连接服务的方式
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    //配置序列化key，value类
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserializathion,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserializathion,
    //自动重置偏移量为最新的偏移量
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //true后台自动提交偏移量，宕机后会丢失数据；false需要手动维护偏移量
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: lang.Boolean)
  )

   /**
   * 创建DStream，返回接收到的输入数据
   * LocationStrategies：根据给定的主题和集群地址创建consumer
   * LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
   * ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
   * ConsumerStrategies.Subscribe：订阅一系列主题
   **/
  def getKafkaDStream(topics: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]]  = {
    //从kafka工具类中创建DirectDStream
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      //位置策略，指定计算的Executor
      LocationStrategies.PreferConsistent,
      //消费策略，指定消费的主题
      ConsumerStrategies.Subscribe[String, String](Set(topics), kafkaParams)
    )
    dStream
  }


}
