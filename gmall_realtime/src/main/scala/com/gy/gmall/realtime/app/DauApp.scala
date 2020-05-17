package com.gy.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.gy.common.constant.GmailConstants
import com.gy.gmall.realtime.bean.StartUpLog
import com.gy.gmall.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputStream = MyKafkaUtil.getKafkaStream(GmailConstants.KAFKA_TOPIC_STARTUP, ssc)

    val StartUpLogStream =  inputStream.map{
      record =>
        val jsonStr: String = record.value()
        val StartUpLog: StartUpLog = JSON.parseObject(jsonStr,classOf[StartUpLog])
        val date = new Date(StartUpLog.ts)
        val dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)

        val dateArr: Array[String] = dateStr.split(" ")
        StartUpLog.logDate = dateArr(0)
        StartUpLog.logHour = dateArr(1).split(":")(0)
        StartUpLog.logHourMinute = dateArr(1)
        StartUpLog
    }

    val filterDStream = StartUpLogStream.transform { rdd =>
      //周期性执行
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "dau:" + curdate
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filterRDD = rdd.filter { StartUpLog =>
        //executor
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(StartUpLog.mid)
      }
      filterRDD
    }

    //去重思路，把相同的mid 的数据分成一组，每组去一个
    val groupByMidDStream = filterDStream.map(StartUpLog => (StartUpLog.mid, StartUpLog)).groupByKey()

    val distinctDStream = groupByMidDStream.flatMap {
      case (mid, startUpLogItr) =>
        startUpLogItr.take(1)
    }

    distinctDStream.foreachRDD(rdd =>
      rdd.foreachPartition(startUpLogItr =>{
        val jedis: Jedis = RedisUtil.getJedisClient
        val list = startUpLogItr.toList
        for(startUpLog <- list){
          val key = "dau:"+startUpLog.logDate
          val value = startUpLog.mid
          jedis.sadd(key,value)
        }


        MyEsUtil.indexBulk(GmailConstants.ES_INDEX_DAU,list)

        jedis.close()
      })
    )

      ssc.start()
      ssc.awaitTermination()

  }
}
