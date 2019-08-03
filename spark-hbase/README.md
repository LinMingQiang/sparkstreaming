# spark-hbase
* spark 1.6.0
* scala 2.10
* hbase 1.2.0
## function
* 提供 SparkHBaseContext 
* spark scan hbase data to RDD <br>
  scan -> RDD[T]
* spark RDD[T] get from hbase to RDD[U] <br>
  RDD[T] -> Get -> RDD[U]
* spark RDD[T] write to hbase <br>
  RDD[T] -> Put -> Hbase
* spark RDD[T] update with hbase data  <br>
  RDD[T] -> Get -> Combine -> RDD[U] <br>
* spark RDD[T] update with hbase data then put return to hbase <br>
  RDD[T] -> Get -> Combine -> Put -> Hbase
## Example
```
    val conf = new SparkConf().setMaster("local").setAppName("tets")
    val sc = new SparkContext(conf)
    val hc = new SparkHBaseContext(sc, zk)
    hc.hbaseRDD(tablename, f).foreach { println }
    hc.scanHbaseRDD(tablename, new Scan(), f)
```
