package com.spark.test

import org.apache.spark.common.util.KafkaConfig

object ConfigurationTest {
  def main(args: Array[String]): Unit = {
    val kconf = new KafkaConfig("", Map("" -> ""))
  }
}
