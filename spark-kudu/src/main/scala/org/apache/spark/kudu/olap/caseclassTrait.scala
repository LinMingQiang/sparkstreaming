package org.apache.spark.kudu.olap

trait caseclassTrait {
  case class smartTable(
    uuid: Long,
    statdate: String,
    hour: String,
    siteid: String,
    plan: String,
    unit: String,
    activity: String,
    channel: String,
    slot: String,
    pubdomain: String,
    creative: String,
    vc:String,
    vcs:String,
    city:String,
    pv: Int,
    click: Int,
    price: Double)
  case class mobileTable(
    uuid: Long,
    statdate: String,
    hour: String,
    siteid: String,
    plan: String,
    unit: String,
    activity: String,
    channel: String,
    slot: String,
    pubdomain: String,
    creative: String,
    pv: Int,
    click: Int,
    price: Double)
}