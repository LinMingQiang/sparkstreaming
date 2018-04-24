package org.apache.spark.common.util

import java.io.File
import java.util.Properties
import java.io.FileInputStream
import java.io.InputStreamReader
import scala.collection.JavaConversions._
object ConfigurationFactoryTool {
  /**
   * @author LMQ
   * @description 初始化conf。
   * @param path ： 配置文件的路径。把配置文件放在项目外部路径以便于修改配置文件不需要重新打包
   */
  def initConf(path: String,conf:Configuration){
    val property = getConfigFromFilePath(path)
    var keys = property.propertyNames()
    while (keys.hasMoreElements()) {
      var key = keys.nextElement().asInstanceOf[String]
      conf.set(key.trim(), property.get(key).toString().trim())
    }
  }
  /**
   * @author LMQ
   * @description 
   */
  def getConfigFromFilePath(filePath: String) = {
    var file = new File(filePath)
    var p = new Properties()
    if (file.exists()) {
      var in = new FileInputStream(file)
      p.load(new InputStreamReader(in))
      in.close()
    } else {
      println(" Configuration Path Is Not Exist : " + filePath)
      System.exit(1)
    }
    p
  }

}