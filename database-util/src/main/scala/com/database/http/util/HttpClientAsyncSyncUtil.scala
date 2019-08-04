package org.reporting.online.deviceenum

import org.apache.http.impl.nio.client.DefaultHttpAsyncClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.params.ClientPNames
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.CountDownLatch
import org.apache.http.client.methods.HttpGet
import org.apache.http.concurrent.FutureCallback
import org.apache.http.HttpResponse
import org.apache.http.util.EntityUtils
import java.util.concurrent.TimeUnit

object HttpClientAsyncSyncUtil {
  var httpclient: DefaultHttpClient = null
  var httpAsyclient: DefaultHttpAsyncClient = null
  /**
   * 获取异步httpclient
   */
  def getHttpAsyncClient(connTime: Int, socketTime: Int) = {
    var httpclient = new DefaultHttpAsyncClient()
    httpclient.getParams()
      .setIntParameter("http.connection.timeout", connTime)
      .setIntParameter("http.socket.timeout", socketTime)
      .setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);
    httpclient
  }
  /**
   * 同步httpclient
   */
  def getHttpDefualtClient(connTime: Int, socketTime: Int) = {
    var httpclient = new DefaultHttpClient()
    httpclient.getParams()
      .setIntParameter("http.connection.timeout", connTime)
      .setIntParameter("http.socket.timeout", socketTime)
    httpclient
  }
  /**
   * 同步httpclient
   */
  def getHttpDefualtClient2(connTime: Int, socketTime: Int) = {
    if (null == httpclient) {
      httpclient = new DefaultHttpClient()
      httpclient.getParams()
        .setIntParameter("http.connection.timeout", connTime)
        .setIntParameter("http.socket.timeout", socketTime)
      httpclient
    }
httpclient
  }
  /**
   * 获取异步httpclient
   */
  def getHttpAsyncClient2(connTime: Int, socketTime: Int) = {
    if (null == httpAsyclient) {
       httpAsyclient = new DefaultHttpAsyncClient()
      httpAsyclient.getParams()
        .setIntParameter("http.connection.timeout", connTime)
        .setIntParameter("http.socket.timeout", socketTime)
        .setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);
      httpAsyclient
    }
httpAsyclient
  }
  def post(sends: ArrayBuffer[String]){
     val httpc=getHttpAsyncClient2(10000, 10000)
     val latch = new CountDownLatch(sends.size)
     httpc.start()
     sends.foreach { url => 
       val get=new HttpGet(url)
     httpc.execute(get, new FutureCallback[HttpResponse]() {
            def error(errMsg: String) {
              try {
                println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+errMsg)
                get.releaseConnection()
                latch.countDown()
              } catch { case t: Throwable => t.printStackTrace() }
            }
            def completed(response: HttpResponse) {
              /*val entity = response.getEntity
              if (entity != null) {
                val str=EntityUtils.toString(entity, "utf-8")
                println(str)
              }*/
              get.releaseConnection()
              latch.countDown()
            }
            def failed(ex: Exception) { error(ex.toString) }
            def cancelled() { error("cancelled") }
          });
     }
     latch.await(2, TimeUnit.MINUTES)//等待最多一分钟。
     httpc.shutdown()
  }
}