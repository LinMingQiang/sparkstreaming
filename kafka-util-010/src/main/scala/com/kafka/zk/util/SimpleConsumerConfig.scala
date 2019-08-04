package com.kafka.zk.util

import java.util.Properties

class SimpleConsumerConfig(brokers: String, originalProps: Properties) {
  val seedBrokers: Array[(String, Int)] = brokers.split(",").map { hp =>
    val hpa = hp.split(":")
    if (hpa.size == 1) {
      (hpa(0), 9092)
    } else {
      (hpa(0), hpa(1).toInt)
    }
  }
}
object SimpleConsumerConfig {
  /**
   * Make a consumer config without requiring group.id or zookeeper.connect,
   * since communicating with brokers also needs common settings such as timeout
   */
  def apply(kafkaParams: Map[String, String]): SimpleConsumerConfig = {
    // These keys are from other pre-existing kafka configs for specifying brokers, accept either
    val brokers = kafkaParams.get("metadata.broker.list")
      .orElse(kafkaParams.get("bootstrap.servers"))
      .getOrElse(throw new Exception(
        "Must specify metadata.broker.list or bootstrap.servers"))

    val props = new Properties()
    kafkaParams.foreach {
      case (key, value) =>
        // prevent warnings on parameters ConsumerConfig doesn't know about
        if (key != "metadata.broker.list" && key != "bootstrap.servers") {
          props.put(key, value)
        }
    }

    Seq("zookeeper.connect", "group.id").foreach { s =>
      if (!props.contains(s)) {
        props.setProperty(s, "")
      }
    }

    new SimpleConsumerConfig(brokers, props)
  }
}