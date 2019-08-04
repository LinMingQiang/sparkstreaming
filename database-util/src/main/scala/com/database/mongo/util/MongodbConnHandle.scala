package com.database.mongo.util

import com.mongodb.MongoClient
import java.util.ArrayList
import com.mongodb.ServerAddress

object MongodbConnHandle {
  var client: MongoClient = null
  /*
   * 
   */
  def getMongoClient(mongourl: String) = {
    initializeMongo(mongourl)
    client
  }
  /*
   * 
   */
  def initializeMongo(mongourl: String) {
    if (client == null) {
      var addresss = new ArrayList[ServerAddress]
      var b = mongourl.split(",")
      for (s <- b) {
        var ss = s.split(":")
        var address = new ServerAddress(ss(0), ss(1).toInt)
        addresss.add(address)
      }
      client = new MongoClient(addresss)
    }
  }
}