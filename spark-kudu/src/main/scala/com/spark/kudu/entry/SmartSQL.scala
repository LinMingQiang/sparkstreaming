package com.spark.kudu.entry

trait SmartSQL {
  def rt_rtbreport_byslot_channel_plan(statdate:String) = s"""
 upsert into rt_rtbreport_byslot_channel_plan
 SELECT
  statdate ,
  siteid ,
  plan ,
  slot ,
  channel ,
  pubdomain ,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog  where statdate='${statdate}' and channel != '' and pubdomain !='' and slot !='' 
 group by statdate,siteid,slot,plan,channel,pubdomain;"""

}