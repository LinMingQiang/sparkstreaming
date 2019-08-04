package com.rabbit.mq.util

import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import com.rabbitmq.client.QueueingConsumer
import com.rabbitmq.client.ConnectionFactory

class RabbitMQConnHandler(var host: String) {
  var connection: Connection = null
  var port: Int = 5672
  var userName: String = "admin"
  var password: String = "admin"
  var connecttiontimeout: Int = 10000
  def this(host: String, port: Int) {
    this(host)
    this.port = port
    initConn
  }
  def this(
    host: String,
    port: Int,
    userName: String,
    password: String) {
    this(host)
    this.port = port
    this.userName = userName
    this.password = password
    initConn
  }
  private def initConn() {
    if (connection == null) {
      val factory = new ConnectionFactory();
      factory.setUsername(userName);
      factory.setPassword(password);
      factory.setHost(host);
      factory.setPort(port);
      factory.setConnectionTimeout(connecttiontimeout);
      try {
        this.connection = factory.newConnection();
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }
  }
  def reInitConn(){
    initConn
  }
  def getExchangeDeclareChannel(
      exchangeName: String,
      exchangetype:String="topic") = {
    val channel = connection.createChannel();
    channel.exchangeDeclare(exchangeName,exchangetype);
    channel.basicQos(1); //公平调度，如果多个消费者消费这个队列，尽量公平发消息，就设置这个
    channel
  }
    def getQueueDeclareChannel(
        queueName: String) = {
    val channel = connection.createChannel();
    channel.queueDeclare(queueName, true, false, false, null);
    channel.basicQos(1); //公平调度，如果多个消费者消费这个队列，尽量公平发消息，就设置这个
    channel
  }
  def close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
  }
}