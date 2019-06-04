# spark-kafka

**scala version 2.11** <br/>
**spark version 2.0.1** <br/>
**kafka version 0.10** <br/>

# 说明

-------------------
> 由于kakfa-010 的api的变化，之前的 kafka-08 版本的 spark-kafka 虽然能用，但是他依赖于spark-streaming-kafka-0-8_2.10 <br/>.(可能会导致一些版本问题)；所以这次重新写了一个 kafka010 & spark-2.x 版本 ；但是使用方法还是跟之前的差不多， <br/>
-------------------

-------------------
> kafka010有两种来管理offset的方式，一种是旧版的用zookeeper来管理，一种是本身自带的。现只提供zookeeper的管理方式
-------------------

-------------------
> 要确保编译的kafka-client的版本和服务器端的版本一致，否则会报 Error reading string of length 27489, only 475 bytes available 等错误<br/>
-------------------


* 提供 对外的 ssc创建 createDirectStream 的方法 ，用来读取kafka的数据。
* 提供 对外的 ssc利用conf创建Dstream的方法
* 提供 使用direct方式读取kafka数据的方法
* 提供  "kafka.consumer.from" -> "LAST"/"CONSUM"/"EARLIEST/CUSTOM" 参数，来动态决定获取kafka数据是从last还是从消费点开始
* 增加 "kafka.consumer.from" -> "CUSTOM" : 可以配置offset在配置文件里增加  kafka.offset= ${offset}
  offset 格式为  topic,partition,offset|topic,partition,offset|topic,partition,offset
  (当然你要自己定义offset也可以。这个在SparkKafkaContext已经提供了相应的方法。需要你传入 fromoffset)
* 提供 "wrong.groupid.from"->"EARLIEST/LAST" 参数 ，决定新group id 或者过期的 group id 从哪开始读取
* 提供 fromoffset 参数，决定从具体的offset开始获取数据
* 提供 rdd 更新kafka offsets到zookeeper的方法
* 提供 rdd 写数据进kakfa方法
* 提供 StreamingKafkaContext，SparkKafkaContext 使用更方便
* 提供 KafkaDataRDD，封装了更新offset等操作在里面。不用再用隐式转换来添加这些功能了
* 提供一个kafkaCluster。可以用来单独获取kafka信息，如最新偏移量等信息
* 修改 增加updateOffsets方法不用提供group id
* 增加 更新偏移量至最新操作。updataOffsetToLastest
* 修改，在kp里面设置spark.streaming.kafka.maxRatePerPartition。


  
# Example StreamingKafkaContextTest
> StreamingKafkaContextTest 流式 
```
 val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
    val ssc = new StreamingKafkaContext(sc, Seconds(5))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "testGroupid",
      StreamingKafkaContext.WRONG_FROM -> "last",//EARLIEST
      StreamingKafkaContext.CONSUMER_FROM -> "consum")
    val topics = Set("testtopic")
    val ds = ssc.createDirectStream[String, String](kp, topics)
    ds.foreachRDD { rdd =>
      println(rdd.count)
      //rdd.foreach(println)
      //do rdd operate....
      ssc.getRDDOffsets(rdd).foreach(println)
      //ssc.updateRDDOffsets(kp,  "group.id.test", rdd)//如果想要实现 rdd.updateOffsets。这需要重写inputstream（之后会加上）
    }
    ssc.start()
    ssc.awaitTermination()
```
# Example StreamingKafkaContextTest With Confguration
> StreamingKafkaContextTest （配置文件，便于管理。适用于项目开发）
```
 val conf = new ConfigurationTest()
    initConf("conf/config.properties", conf)
    initJobConf(conf)
    println(conf.getKV())
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingKafkaContext(sc, Seconds(5))
    val ds = ssc.createDirectStream(conf)
    ds.foreachRDD { rdd => rdd.foreach(println) }
    ssc.start()
    ssc.awaitTermination()

```
# Example SparkKafkaContext 
> SparkKafkaContext （适用于离线读取kafka数据）
```
val skc = new SparkKafkaContext(
      new SparkConf().setMaster("local").setAppName("SparkKafkaContextTest"))
    val kp =Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    val kafkadataRdd = skc.kafkaRDD[String, String](kp, topics, msgHandle)
    kafkadataRdd.foreach(println)
    kafkadataRdd.updateOffsets(kp)//更新kafka偏移量
    
```
# Example KafkaWriter 
> KafkaWriter （将rdd数据写入kafka）
```
 val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
    val ssc = new StreamingKafkaContext(sc, Seconds(5))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "kafka.last.consum" -> "consum")
    val topics = Set("test")
    val ds = ssc.createDirectStream[String, String](kp, topics)
    ds.foreachRDD { rdd => 
      rdd.foreach(println)
      rdd.map(_._2)
         .writeToKafka(producerConfig, transformFunc(outTopic,_))
      }

    ssc.start()
    ssc.awaitTermination()
    
    
```
