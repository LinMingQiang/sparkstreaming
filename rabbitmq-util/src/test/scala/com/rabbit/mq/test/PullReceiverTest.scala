package com.rabbit.mq.test

import com.rabbit.mq.util.RabbitMQConnHandler
import com.rabbit.mq.util.RabbitMQProducer
import com.rabbit.mq.util.RabbitMQPullReceiver
object PullReceiverTest {
   def main(args: Array[String]): Unit = {
    testConsumer
  }
  def testConsumer() {
    val host = "192.168.10.229"
    val port = 5672
    val username = "admin"
    val passw = "admin"
    val exchangeName = "test"
    val mqHandler = new RabbitMQConnHandler(host, port, username, passw)
    var consumerChannel = mqHandler.getQueueDeclareChannel(exchangeName)
    val consumer = new RabbitMQPullReceiver(consumerChannel, exchangeName)
    while (true) {
      val r = consumer.receiveMessage()
      if (r.isRight) {
        val (msg, deliveryTag) = r.right.get
        if (deliveryTag > 0) {
          println("msg : ", msg)
          consumer.basicAck(deliveryTag)
        }else {
          Thread.sleep(1000)
        }
      } else {
        //报错
        if(!mqHandler.connection.isOpen()){
          mqHandler.reInitConn
        }
        if(!consumerChannel.isOpen()){
          consumerChannel = mqHandler.getQueueDeclareChannel(exchangeName)
        }
      }
     
    }
    consumer.close()
    mqHandler.close()
  }
}