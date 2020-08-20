package com.myexercise3.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author: Zhangbao
 * @Date: 0:45 2020/8/17
 * @Description:
 *
 *  properties工具类：
 *  用来装载properties文件
 *
 */
object PropertiesUtil3 {
  //获取properties已装载数据流的对象
  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    //为properties选择Input数据流
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }
}
