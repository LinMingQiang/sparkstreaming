package com.database.es.util

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.action.search.SearchResponse
/**
 * @time : 2018-01-06
 * @func : 提供es的各种查询功能
 */
trait ElasticserachQueryFunc {
  val timecalue = new TimeValue(100000000)
  /**
   * @time :2018-02-06
   * @function :用于获取es的分页数据
   * @param scrollId:分页的scrollId。可从getESPageResponseQuery中获取response后得
   */
  def getESPageQuery(
    client: TransportClient,
    scrollId: String): SearchResponse = {
    client
      .prepareSearchScroll(scrollId)
      .setScroll(timecalue)
      .execute()
      .actionGet()
  }
  /**
   * @time :2018-02-06
   * @function :用于获取es的分页数据
   */
  def getESPageQuery(
    client: TransportClient,
    response: SearchResponse
    ): SearchResponse = {
    client
      .prepareSearchScroll(response.getScrollId)
      .setScroll(timecalue)
      .execute()
      .actionGet()
  }
  /**
   * @time :2018-02-06
   * @function :用于获取es的分页数据。用于首次获取response的信息。
   * @param scrollSize:分页条数
   */
  def getESPageQuery(
    client: TransportClient,
    query: QueryBuilder,
    index: String,
    indexType: String,
    scrollSize: Int
    ): SearchResponse = {
    val firsttimersp = client.prepareSearch(index)
      .setScroll(timecalue)
      .setTypes(indexType)
      .setSize(scrollSize)
      .setQuery(query) //根据条件查询,支持通配符大于等于0小于等于19  
      .get()
    getESPageQuery(client, firsttimersp)
  }
    /**
   * @time :2018-02-06
   * @function :用于获取es的分页数据。用于首次获取response的信息。
   * @param scrollSize:分页条数
   */
  def getESPageQuery(
    client: TransportClient,
    query: String,
    index: String,
    indexType: String,
    scrollSize: Int
    ): SearchResponse = {
    val firsttimersp = client.prepareSearch(index)
      .setScroll(timecalue)
      .setTypes(indexType)
      .setSize(scrollSize)
      .setQuery(query) //根据条件查询,支持通配符大于等于0小于等于19  
      .get()
    getESPageQuery(client, firsttimersp)
  }
  /**
   * @time :2018-02-06
   * @function :用于获取es的分页数据。用于首次获取response的信息。
   * @param scrollSize:分页条数
   */
  def getESPageResponseQuery(
    client: TransportClient,
    query: String,
    index: String,
    indexType: String,
    scrollSize: Int) = {
    client.prepareSearch(index)
      .setScroll(timecalue)
      .setTypes(indexType)
      .setSize(scrollSize)
      .setQuery(query) //根据条件查询,支持通配符大于等于0小于等于19  
      .get()
  }
  /**
   * @time :2018-02-06
   * @function :用于获取es的分页数据。用于首次获取response的信息。
   * @param scrollSize:分页条数
   */
  def getESPageResponseQuery(
    client: TransportClient,
    query: QueryBuilder,
    index: String,
    indexType: String,
    scrollSize: Int) = {
    client.prepareSearch(index)
      .setScroll(timecalue)
      .setTypes(indexType)
      .setSize(scrollSize)
      .setQuery(query) //根据条件查询,支持通配符大于等于0小于等于19  
      .get()
  }

}