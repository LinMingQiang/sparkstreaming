# spark-kafka
	<groupId>spark-kafka</groupId>
	<artifactId>spark-kafka-0-10</artifactId>
	<version>2.0.1</version>
	<properties>
		<maven.compiler.source>1.6</maven.compiler.source>
		<maven.compiler.target>1.6</maven.compiler.target>
		<encoding>UTF-8</encoding>
		<scala.version>2.11.5</scala.version>
		<spark.vertion>2.0.1</spark.vertion>
		<kafka.version>0.10.0.0</kafka.version>
		<scala.compat.version>2.11</scala.compat.version>
	</properties>
更多的util -> https://github.com/LinMingQiang/spark-util
# 说明

-------------------
> - 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，StructuredStreaming中使用trigger实现了。） <br/>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> - 支持rdd.updateOffset 来管理偏移量。 <br/>
-------------------

-------------------
> - 由于kakfa-010 的api的变化，之前的 kafka-08 版本的 spark-kafka 虽然能用，但是他依赖于spark-streaming-kafka-0-8_2.10 <br/>.(可能会导致一些版本问题)；所以这次重新写了一个 kafka010 & spark-2.x 版本 ；但是使用方法还是跟之前的差不多， <br/>
-------------------

-------------------
> - kafka010有两种来管理offset的方式，一种是旧版的用zookeeper来管理，一种是本身自带的。现只提供zookeeper的管理方式
-------------------

-------------------
> - 要确保编译的kafka-client的版本和服务器端的版本一致，否则会报 Error reading string of length 27489, only 475 bytes available 等错误<br/>
-------------------

-------------------
> - 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。<br> 
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


  
#  StreamingDynamicContext
> 动态调整批次时间的流式计算（不基于streamingContext）
> 例如设置批次时间为5s钟，StreamingContex 是严格的5s钟一次。 而StreamingDynamicContext 可以由用户设定，在当前批次任务完成后，是否马上启动下一个批  次的计算。。建议可以根据 当前批次的rdd.cou > 0 来判断是否马上执行下一批次
```
    val kp = SparkKafkaContext.getKafkaParam( brokers,groupId,"consum", "last")
    val topics = Set("test")
    val skc = new SparkKafkaContext(kp,new SparkConf()
    .setMaster("local")
    .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "10")  //kafka每个分区最大拉区数量
    .setAppName("SparkKafkaContextTest"))
    val sskc = new StreamingDynamicContext(skc, Seconds(2))   //如果 kafka没有数据则等待2s。否则马上执行下一批次任务
    val kafkastream = sskc.createKafkaDstream[String, String, StringDecoder, StringDecoder, (String, String)](topics, msgHandle)
    kafkastream.foreachRDD { case (rdd) =>
       rdd.map(x => x._2).collect().foreach { println }
       rdd.count > 0 //是否马上执行下个批次。否则就等到下一批次时间到来，如果 count>0 说明kafka有数据，可以马上执行而不需要等待浪费时间，让你的流式更实时
    }
    sskc.start()
```
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
