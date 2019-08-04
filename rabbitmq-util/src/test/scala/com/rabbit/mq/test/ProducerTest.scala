package com.rabbit.mq.test
import com.rabbit.mq.util.RabbitMQConnHandler
import com.rabbit.mq.util.RabbitMQProducer
import com.rabbit.mq.util.RabbitMQPullReceiver
import java.util.Date

object ProducerTest {
  def main(args: Array[String]): Unit = {
    testProducer
  }
  def testProducer = {
    val host = "192.168.10.229"
    val port = 5672
    val username = "admin"
    val passw = "admin"
    val exchangeName = "test"
    val mqHandler = new RabbitMQConnHandler(host, port, username, passw)
    val sendChannel = mqHandler.getQueueDeclareChannel(exchangeName)
    val producer = new RabbitMQProducer(sendChannel, exchangeName)
    while (true) {
      val msg = new Date().toString()
      producer.sendQueueMsg(exchangeName, msg)
      println(msg)
      Thread.sleep(500)
    }
    producer.close()
    mqHandler.close()
  }
}