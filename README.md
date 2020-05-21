# spark-kafka
     <groupId>org.spark.kafka</groupId>
     <artifactId>spark-kafka-0-8</artifactId>
     <version>${spark-vertion}</version>
 	  <properties>
		<maven.compiler.source>1.6</maven.compiler.source>
		<maven.compiler.target>1.6</maven.compiler.target>
		<encoding>UTF-8</encoding>
		<scala.version>2.10.6</scala.version>
		<spark.version>1.6.0</spark.vertion>
		<kafka.version>0.8.2.0</kafka.version>
		<scala.compat.version>2.10</scala.compat.version>
	</properties>
更多的util -> https://github.com/LinMingQiang/spark-util
## POINT
---------------------
> - 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，StructuredStreaming中使用trigger实现了。） <br/>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。 
> - 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> - 支持rdd.updateOffset 来管理偏移量。 <br/>
--------------------
  
* 封装 StreamingDynamicContext 来实现动态调整 流式的批次
* 可在sparkstreaming的过程中 动态地修改topic 
* 添加了 sparkStreaming 1.6 -> kafka 010  的 spark-streaming-kafka-0-10_2.10 。用以支持ssl 。
* 提供 自定义的 StreamingKafkaContext创建 createDirectStream 的方法 ，用来读取kafka的数据。
* 提供 自定义的 StreamingKafkaContext利用conf创建Dstream的方法
* 提供 使用direct方式读取kafka数据的方法
* 提供  "kafka.consumer.from" -> "LAST"/"CONSUM"/"EARLIEST/CUSTOM" 参数，来动态决定获取kafka数据是从last还是从消费点开始
* 增加 "kafka.consumer.from" -> "CUSTOM" : 可以配置offset在配置文件里增加  kafka.offset= ${offset}
  offset 格式为  topic,partition,offset|topic,partition,offset|topic,partition,offset
  (当然你要自己定义offset也可以。这个在SparkKafkaContext已经提供了相应的方法。需要你传入 fromoffset)
* 提供 "wrong.groupid.from"->"EARLIEST/LAST" 参数 ，决定新group id 或者过期的 group id 从哪开始读取
* 提供 fromoffset 参数，决定从具体的offset开始获取数据
* 提供 rdd 更新kafka offsets到zookeeper的方法（rdd.updateOffsets(kp)）
* 提供 rdd 写数据进kakfa的隐式函数(rdd.writeToKafka)
* 提供 StreamingKafkaContext，SparkKafkaContext 使用更方便
* 提供 KafkaDataRDD，封装了更新offset等操作在里面。不用再用隐式转换来添加这些功能了（rdd.updateOffsets(kp)）
* 提供一个SparkKafkaUtil。可以用来单独获取kafka信息，如最新偏移量等信息
* 增加 更新偏移量至最新操作。updataOffsetToLastest
* 修改，在kp里面设置spark.streaming.kafka.maxRatePerPartition。
* 支持 topic新增分区数时的offset问题。（默认是从 0 开始 ）

#  StreamingDynamicContext
> 动态调整批次时间的流式计算（不基于streamingContext）
> 例如设置批次时间为5s钟，StreamingContex 是严格的5s钟一次。 而StreamingDynamicContext 可以由用户设定，在当前批次任务完成后，是否马上启动下一个批  次的计算。。建议可以根据 当前批次的rdd.cou > 0 来判断是否马上执行下一批次
```
    val kp = SparkKafkaContext.getKafkaParam( brokers,groupId,"consum", "last")
    val skc = new SparkKafkaContext(kp,new SparkConf().set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "10")  //kafka每个分区最大拉区数量
    val sskc = new StreamingDynamicContext(skc, Seconds(2)) //如果 kafka没有数据则等待2s。否则马上执行下一批次任务
    val kafkastream = sskc.createKafkaDstream[String, String, StringDecoder, StringDecoder, (String, String)](topics, msgHandle)
    kafkastream.foreachRDD { case (rdd) =>
    rdd.map(x => x._2).collect().foreach { println }
    rdd.count > 0 //是否马上执行下个批次。否则就等到下一批次时间到来，如果 count>0 说明kafka有数据，可以马上执行而不需要等待浪费时间，让你的流式更实时
    }
    sskc.start()
```
  
#  StreamingKafkaContext
> StreamingKafkaContext 流式 
```
   val kp = SparkKafkaContext.getKafkaParam( brokers,groupId,"consum", "last")
    val sc = new SparkContext(new SparkConf())
    val ssc = new StreamingKafkaContext(kp,sc, Seconds(5))
    val ds = ssc.createDirectStream[String,String,StringDecoder,StringDecoder,((String, Int, Long), String)](topics, msgHandle2)
    ds.foreachRDD { rdd =>}
    ssc.start()
    ssc.awaitTermination()
```
#  StreamingKafkaContext With Confguration
> StreamingKafkaContext （配置文件，便于管理。适用于项目开发）
```
     val kp = SparkKafkaContext.getKafkaParam( brokers,groupId,"consum", "last")
    val conf = new KafkaConfig("conf/config.properties",kp)
    conf.setTopics(topics)
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingKafkaContext(kp,sc, Seconds(5))
    val ds = ssc.createDirectStream[
      String,String,StringDecoder,StringDecoder,((String, Int, Long), String)](
          conf, msgHandle2)
    ds.foreachRDD { rdd => rdd.foreach(println) }
    ssc.start()
    ssc.awaitTermination()
```
#  SparkKafkaContext 
> SparkKafkaContext （适用于离线读取kafka数据）
```
    val kp = SparkKafkaContext.getKafkaParam( brokers,groupId,"consum", "last")
    val skc = new SparkKafkaContext(kp,new SparkConf())
    val kafkadataRdd = skc.kafkaRDD[((String, Int, Long), String)](topics, msgHandle2) //根据配置开始读取
    kafkadataRdd.getRDDOffsets().foreach(println)
    kafkadataRdd.updateOffsets(kp)
```
#  KafkaWriter 
> KafkaWriter （将rdd数据写入kafka）
```
   val sc = new SparkContext(new SparkConf())
   val kp = SparkKafkaContext.getKafkaParam( brokers,groupId,"consum", "last")
   val ssc = new StreamingKafkaContext(kp,sc, Seconds(5))
   val ds = ssc.createDirectStream(topics)
   ds.foreachRDD { rdd =>rdd.map(_._2) .writeToKafka(producerConfig, transformFunc(outTopic, _))}
   ssc.start()
    ssc.awaitTermination()
    
```
