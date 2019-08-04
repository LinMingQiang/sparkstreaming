package com.rabbit.mq.util

import com.rabbitmq.client.Channel
import com.rabbitmq.client.QueueingConsumer

class RabbitMQConsumer {

  var exchangeName: String = null
  var channel: Channel = null
  var consumer: QueueingConsumer = null
  def this(channel: Channel, exchangeName: String) {
    this()
    this.channel = channel
    this.exchangeName = exchangeName
    initConsumer
  }
  private def initConsumer() {
    if (consumer == null) {
      try {
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(exchangeName, false, consumer);
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }
  }
  def receiveMessage():Either[Throwable,(String,Long)]={
    try {
      val delivery = consumer.nextDelivery();
      if (delivery != null) {
        val msg = new String(delivery.getBody())
        val deliveryTag = delivery.getEnvelope().getDeliveryTag()
        Right((msg, deliveryTag))
      } else {
        Right((null, -10312))
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
       Left(t)
    }

  }
  def basicAck(deliveryTag: Long) = {
    try {
      channel.basicAck(deliveryTag, false);
      true
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        false
    }
  }
  def close() {
    try {
      if (channel != null) {
        channel.close();
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
  }
}