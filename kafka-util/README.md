# 用于获取kafka每个topic的偏移量并记录至zk <br>
scala version 2.10 <br>
kafka version 0.8 <br>

* 主要是用于记录kafka各个topic的偏移量的，以方便每天的重算等，建议每天凌晨0点的时候运行一次，如果不放心，可以没小时执行一次 <br>
* 支持的功能：支持按日期来存储topic的offset到zookeeper上
* 提供 recordDayOffsetsToZK 来将当天最新的 offset 写入zk对应路径下
* 提供recordDayHourOffsetToZK 来按 每天每小时来记录topic的offset
# Example 
> 记录某天的topic 的最新offset
```
    val groupid = "kafkadayoffset"
    val day = "20180115"
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> groupid)
    val kafkaoffsetUtil = KafkaOffsetUtil(kafkaParams, zk)
    kafkaoffsetUtil.recordDayOffsetsToZK(day, topics)//记录当天的offset
    val res = kafkaoffsetUtil.getDayOffsetsFromZK(topics, day)//拉取某天的offset
    if (res.isLeft) 
      println(res.left.get)
     else 
    res.right.get.foreach(println)
```

> 记录某天某时的topic 的最新offset
```
    val groupid = "kafkadayoffset"
    val day = "20180115"
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> groupid)
    val kafkaoffsetUtil = KafkaOffsetUtil(kafkaParams, zk)
    kafkaoffsetUtil.recordDayHourOffsetToZK(day,hour,topics)
    val res = kafkaoffsetUtil.getDayHourOffsetsFromZK(day, hour, topics)
    if (res.isLeft) 
      println(res.left.get)
     else 
    res.right.get.foreach(println)
```