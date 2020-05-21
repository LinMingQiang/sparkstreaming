# spark-kafka-0-10
## Support
---
|                    | scala version      |Kafka version       | 
|:------------------:|:------------------:|:------------------:|
| **spark 2.0.x**    | 2.10/2.11          | 0.10.2.1- | 
---
-------------------
> - 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，StructuredStreaming中使用trigger实现了。） <br/>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 支持 kafka的ssl 。 通过 --file 的方式<br/>
> - 支持读取 kafka数据为RDD，支持 rdd.updateOffset 来管理偏移量。 <br/>
-------------------

-------------------
> - kafka010有两种来管理offset的方式，一种是旧版的用zookeeper来管理，一种是本身自带的。现只提供zookeeper的管理方式
-------------------

-------------------
> - 要确保编译的kafka-client的版本和服务器端的版本一致，否则会报 Error reading string of length 27489, only 475 bytes available 等错误<br/>
-------------------
-------------------
> - 添加了速率控制，KafkaRateController。用来控制读取速率。<br>
-------------------
