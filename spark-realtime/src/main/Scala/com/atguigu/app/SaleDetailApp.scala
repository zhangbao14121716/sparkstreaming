package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization


/**
 * @Author: Zhangbao
 * @Date: 9:03 2020/8/21
 * @Description:
 *
 */
object SaleDetailApp {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConfig配置信息，StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //2.获取kafkaDStream，order order_detail
    val orderInfoOriginDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER, ssc)
    val orderDetailOriginDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, ssc)
    //3.对两条流结构进行处理
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOriginDStream.map(record => {
      //
      val orderOriginInfo: String = record.value()
      //
      val orderInfo: OrderInfo = JSON.parseObject(orderOriginInfo, classOf[OrderInfo])
      //new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(orderInfo.create_time))
      orderInfo.create_date = orderInfo.create_time.toString.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.toString.split(" ")(1).split(":")(0)
      orderInfo
    })
    val orderDetailDStream: DStream[OrderDetail] = orderDetailOriginDStream.map(record => {
      val orderDetailOrigin: String = record.value()
      JSON.parseObject(orderDetailOrigin, classOf[OrderDetail])
    })
    //4.就转换后的流添加上Key值
    val idToOrderInfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.map(orderInfo => {
      (orderInfo.id, orderInfo)
    })
    val idToOrderDetailDStream: DStream[(String, OrderDetail)] = orderDetailDStream.map(orderDetail => {
      (orderDetail.order_id, orderDetail)
    })
    //5.用fullOuterJoin获取所有流信息
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(idToOrderDetailDStream)
    //
    val saleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(iter => {
      //5.1获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //5.2SaleDetail存入缓存
      val saleDetailList: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
      iter.foreach { case (orderId, (orderInfoOpt, orderDetailOpt)) => {
        //隐式转换:JSON for Scala
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
        //1.infoRedisKey和detailRedisKey
        val infoRedisKey: String = s"orderInfo:$orderId"
        val detailRedisKey: String = s"orderDetail:$orderId"
        //分支一:都不为空的写入结果集
        //info流，isDefined有值，是否Some类型
        if (orderInfoOpt.isDefined) {
          //info流非空，获取值
          val orderInfoData: OrderInfo = orderInfoOpt.get
          if (orderDetailOpt.isDefined) {
            //detail流非空，获取值
            val orderDetailData: OrderDetail = orderDetailOpt.get
            val saleDetail: SaleDetail = new SaleDetail(orderInfoData, orderDetailData)
            saleDetailList += saleDetail
          }

          //分支二：“一”对多中，“一”写入redis等待被查询,保存100s
          //--JSON对象，无法用java的fastjson转换成string类型
          //--error: jedisClient.set(s"orderInfo:$orderId", orderInfoData.toString)
          val orderInfoJsonString: String = Serialization.write(orderInfoData)
          jedisClient.setex(infoRedisKey, 100, orderInfoJsonString)

          //分支三：查询另一个流的缓存,获取当前orderId的orderDetail缓存
          val orderDetailSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          //--需要将java.set转换成scala.set
          import collection.JavaConverters._
          orderDetailSet.asScala.foreach(orderDetailString => {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
            saleDetailList += new SaleDetail(orderInfoData, orderDetail)
          })
        } else { //orderInfo为空值,orderDetail一定不为空值
          val orderDetail: OrderDetail = orderDetailOpt.get
          //查询另一个流的缓存，获取当前orderId的orderInfo缓存
          val orderInfoString: String = jedisClient.get(infoRedisKey)
          if (orderInfoString != null) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
            saleDetailList += new SaleDetail(orderInfo, orderDetail)
          }else {
            //将没有与前置批次join上orderDetail的写入缓存
            val orderDetailString: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, orderDetailString)
            jedisClient.expire(detailRedisKey, 100)
          }
        }
      }
      }
      //5.3释放连接
      jedisClient.close()
      //返回iterator
      saleDetailList.toIterator
    })
    //6.打印预览
    saleDetailDStream.print(100)
    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}
