package com.atguigu.utils

import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
 * @Author: Zhangbao
 * @Date: 18:41 2020/8/19
 * @Description:
 *
 */
object MyESUtil {
  private val ES_HOST: String = "http://hadoop103"
  private val ES_HTTP_PORT: Int = 9200
  private var factory: JestClientFactory = _

  /**
   * 获取客户端
   */
  def getClient:JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 创建工厂
   */
  def build():JestClientFactory = {
    factory = new JestClientFactory
    val config: HttpClientConfig = new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(10000)
      .build()
    factory.setHttpClientConfig(config)
    factory
  }

  /**
   * 关闭客户端
   */
  def close(client: JestClient) ={
    if (!Objects.isNull(client))
      try {
        client.shutdownClient()
      } catch {
        case e:Exception =>
          e.printStackTrace()
      }
  }

  /**
   *
   * @param indexName:索引名
   * @param typeName:类型名
   * @param list:(docId,批量数据)
   * @return
   */
  def insertByBulk(indexName:String, typeName:String, list: List[(String, Any)]) = {

    //a.获取连接
    val client: JestClient = getClient
    //b.创建Bulk
    val builder: Bulk.Builder = new Bulk.Builder()
      .defaultIndex(indexName)
      .defaultType(typeName)
    //c.循环遍历List, 为每条数据创建docId
    list.foreach{case(docId, couponInfo) => {
      //为每一条数据创建Index对象
      val index: Index = new Index.Builder(couponInfo)
        .id(docId)
        .build()
      builder.addAction(index)
    }}
    val bulk: Bulk = builder.build()
    //d.执行操作
    client.execute(bulk)
    //e.关闭连接
    close(client)
  }
}
