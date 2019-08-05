package com.spark.kudu.entry

import java.sql.DriverManager
import java.util.Date

object KuduImpalaUtil {
  val IMPALAD_HOST = "192.168.10.194";
  val IMPALAD_JDBC_PORT = "21050";
  val CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
  val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  Class.forName(JDBC_DRIVER_NAME);
  val con = DriverManager.getConnection(CONNECTION_URL);
  val stmt = con.createStatement();
  def execute(sql:String){
    stmt.execute(sql)
  }

}
