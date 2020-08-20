package com.atguigu.handler

import java.{lang, util}
import java.time.LocalDate

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
/**
 * @Author: Zhangbao
 * @Date: 14:20 2020/8/15
 * @Description:
 *
 * 处理需求的相关操作
 *
 */
object Dauhandler {


  //根据Redis中保存的数据进行跨批次去重（去重力度大的方案应该先执行，减少后面去重方案的压力）
  def filterByRedis(startLogDStream: DStream[StartUpLog],sc: SparkContext): DStream[StartUpLog] = {
    //方案一:单条数据过滤
    val filterPerStartLogDStream: DStream[StartUpLog] = startLogDStream.filter(startLog => {
      //a.获取Redis客户端连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询Redis中是否存在该Mid
      val exist: lang.Boolean = jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      //c.归还连接
      jedisClient.close()
      //d.存在忽略，过滤出不存在的
      !exist
    })
    filterPerStartLogDStream

    //方案二:使用分区代替单条操作，减少连接数
    val filterPerPartitonDStream: DStream[StartUpLog] = startLogDStream.mapPartitions(iter => {
      //a.获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.过滤数据
      val filterIter: Iterator[StartUpLog] = iter.filter(startLog => {
        !jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      })
      //c.归还连接
      jedisClient.close()
      //d.返回分区的迭代
      filterIter
    })
    filterPerPartitonDStream

    //方案三（优化）:每个批次获取Redis中Set集合数据，广播到Executor
    val filterByBroadcastDStream: DStream[StartUpLog] = startLogDStream.transform(rdd => {

      //a.获取Redis客户端连接，每个批次在Driver执行一次
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.获取本地当前时间的字符串
      val todayString: String = LocalDate.now().toString
      //c.获取Redis中Set集合，当前时间段的key中的所有members
      val midSet: util.Set[String] = jedisClient.smembers(s"dau:${todayString}")
      //d.在SparkContext中创建广播变量
      val midBroadcast: Broadcast[util.Set[String]] = sc.broadcast(midSet)
      //e.归还连接
      jedisClient.close()
      //f.在Executor端使用广播变量进行去重
      rdd.filter(startLog => {
        !midBroadcast.value.contains(startLog.mid)
      })
    })
    filterByBroadcastDStream
  }

  //对第一次跨批次去重后的数据进行同批次去重
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {
    //a.转换结构(K,V)
    val midDateToStartLogDStream: DStream[(String, StartUpLog)] = filterByRedisDStream.map(startLog => {
      (s"${startLog.mid}-${startLog.logDate}", startLog)
    })
    //b.按照K分组，相同mid-date进入同一个分组
    val midDateToStartLogIterDStream: DStream[(String, Iterable[StartUpLog])] = midDateToStartLogDStream.groupByKey()
    //c.对每条<K,V>即每个分组的V进行排序取第一条
    val midDateToStartLogListDStream: DStream[(String, List[StartUpLog])] = midDateToStartLogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    //d.压平，并返回StartUpLog类型的DStream
    midDateToStartLogListDStream.flatMap { case (_, list) =>
      list
    }
  }

  //将两次去重后的startLog的Mid写入Redis
  def saveMidToRedis(filterByGroupDStream:DStream[StartUpLog]): Unit = {
    //对DStream转化成RDD进行操作
    filterByGroupDStream.foreachRDD(rdd => {
      //使用foreachPartition代替foreach,减少连接与释放次数
      rdd.foreachPartition(iter => {
        //a.获取Redis客户端连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.操作将数据写入Redis数据库
        iter.foreach(startLog => {
          val radisKey: String = s"dau:${startLog.logDate}"
          jedisClient.sadd(radisKey,startLog.mid)
        })
        //c.归还连接
        jedisClient.close()
      })
    })
  }

  //将两次去重后的数据保存到HBase
  def saveStartLogToHBase(filterByGroupDStream:DStream[StartUpLog],tableName:String): Unit = {

    //对DStream转化成RDD进行操作
    filterByGroupDStream.foreachRDD(rdd => {
      val cols: Array[String] = classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase())
      rdd.saveToPhoenix(tableName,
        cols,HBaseConfiguration.create(),
        Some("hadoop103,hadoop102,hadoop104:2181"))
    })
  }
}
