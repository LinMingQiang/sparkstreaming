# Spark-Util 
language:Scala <br>
SparkV:1.6.0 <br>
# Spark Kafka Util
SparkStreaming使用Direct的方式获取Kafka数据。更新Zookeeper的Offset <br>
详情请查看branch ：spark-kafka-util <br>
# 示例代码（具体信息查看 相应的branch）
```
 val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
 val ssc = new StreamingContext(sc, Seconds(5))
 //kafka param
 var kp = Map[String, String]("metadata.broker.list" -> brokers,"serializer.class" -> "kafka.serializer.StringEncoder",
                              "group.id" -> "group.id","kafka.last.consum" -> "last")
 val topics = Set("test")
//不适用conf方式
ssc.createDirectStream[(String, String)](kp, topics, fromoffsets, msgHandle)
//配置集中在conf里面
ssc.createDirectStream(conf, fromoffsets, msgHandle)
```

