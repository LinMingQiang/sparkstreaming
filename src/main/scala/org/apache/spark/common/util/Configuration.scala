package org.apache.spark.common.util

import java.util.HashMap
import java.io.File
import java.io.FileInputStream
import java.util.Properties
import java.io.InputStreamReader

class Configuration extends Serializable {
  private var conf: HashMap[String, String] = new HashMap[String, String]
  def containsKey(key: String) = conf.containsKey(key)

  def getString(key: String, default: String) = {
    if (conf.containsKey(key)) conf.get(key)
    else default
  }
  def getInt(key: String, default: Int) = {
    if (conf.containsKey(key)) conf.get(key).toInt
    else default
  }
  def set(key: String, value: String) {
    conf.put(key, value)
  }
  def setInt(key: String, value: Int) {
    conf.put(key, value + "")
  }
  def get(key: String, default: String) = {
    getString(key, default)
  }
  def get(key: String) = {
    getString(key, "")
  }
  def apply(key: String) = {
    getString(key, "")
  }
  def getKV() = {
    conf
  }
  def getBoolean(key: String, d: Boolean) = {
    if (conf.containsKey(key)) conf.get(key).toBoolean
    else d
  }
}
object Configuration {
  def apply(path: String) = {
    val conf = new Configuration
    initConf(path, conf)
    conf
  }

  /**
    * @author LMQ
    * @description 初始化conf。
    * @param path ： 配置文件的路径。把配置文件放在项目外部路径以便于修改配置文件不需要重新打包
    */
  def initConf(path: String, conf: Configuration) {
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
