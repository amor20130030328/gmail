package com.gy.common.util


import java.util
import java.util.Objects

import com.google.gson.GsonBuilder
import com.gy.common.constant.GmailConstants
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import org.apache.commons.beanutils.BeanUtils

object MyESUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null
  private var jestClient : JestClient = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    build()
    factory.getObject
  }


  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  def executeIndexBulk(indexName:String ,list:List[Any], idColumn:String): Unit ={
    val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for ( doc <- list ) {

      val indexBuilder = new Index.Builder(doc)
      if(idColumn!=null){
        val id: String = BeanUtils.getProperty(doc,idColumn)
        indexBuilder.id(id)
      }
      val index: Index = indexBuilder.build()
      bulkBuilder.addAction(index)
    }
    val jestclient: JestClient =  getClient

    val result: BulkResult = jestclient.execute(bulkBuilder.build())
    if(result.isSucceeded){
      println("保存成功:"+result.getItems.size())
    }

  }


  case class Source(mid:Int,name:String,age:Int)

  def main(args: Array[String]): Unit = {

    val c1 = Source(123,"张三",23)
    val c2 = Source(123,"李四",23)
    val c3 = Source(123,"王五",23)

    val site: List[Source] = List(c1,c2,c3)
    println(site.size+"条将要保存")
    indexBulk("sb",site)

  }



  /***
    * 批量插入es
    * @param indexName
    * @param list
    */
  def indexBulk(indexName:String ,list :List[Any]): Unit ={
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for (doc <- list ) {
      val index: Index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }

    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
    println(items.size()+"保存")
    close(jest)
  }
}
