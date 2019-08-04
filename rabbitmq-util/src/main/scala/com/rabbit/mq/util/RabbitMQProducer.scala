package com.rabbit.mq.util
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import com.rabbitmq.client.QueueingConsumer
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties

class RabbitMQProducer(channel: Channel, exchangeName: String) {
  def sendQueueMsg(
      exchangeName: String, 
      messageBodyBytes: String) {
    channel.basicPublish("", 
        exchangeName,
        MessageProperties.PERSISTENT_TEXT_PLAIN,
        messageBodyBytes.getBytes)
  }
  def sendExchangeMsg(
      exchangeName: String, 
      routingKey: String, 
      messageBodyBytes: String){
    channel.basicPublish(
        exchangeName, 
        routingKey, null,
        messageBodyBytes.getBytes)
  }
  def close(){
    if(channel!=null){
      channel.close()
    }
  }
}