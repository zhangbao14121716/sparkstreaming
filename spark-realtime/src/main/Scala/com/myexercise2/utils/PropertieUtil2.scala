package com.myexercise2.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author: Zhangbao
 * @Date: 8:15 2020/8/18
 * @Description:
 *  用来装载properties文件
 *  识别配置信息
 */
object PropertieUtil2 {

  def load(propertiesName: String) : Properties = {
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
    prop
  }
}
