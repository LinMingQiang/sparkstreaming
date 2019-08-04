# RabbitMQ 的 Consumer 和 Producer 的 工具类 <br>
MQ version 3.6.1 <br>

# Example Producer
```
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

```

# Example Consumer
```
  def testConsumer() {
    val host = "192.168.10.229"
    val port = 5672
    val username = "admin"
    val passw = "admin"
    val exchangeName = "test"
    val mqHandler = new RabbitMQConnHandler(host, port, username, passw)
    var consumerChannel = mqHandler.getQueueDeclareChannel(exchangeName)
    val consumer = new RabbitMQConsumer(consumerChannel, exchangeName)
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

```