package com.gy.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.gy.common.constant.GmailConstants
import com.gy.common.util.MyESUtil
import com.gy.gmall.realtime.bean.StartUpLog
import com.gy.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauAppTest {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmailConstants.KAFKA_TOPIC_STARTUP,ssc)

    val StartUpLogStream: DStream[StartUpLog] = inputDStream.map { record =>
      val jsonStr = record.value()
      val StartUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
      val date = new Date(StartUpLog.ts)
      val dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
      val dateArr: Array[String] = dateStr.split(" ")
      StartUpLog.logDate = dateArr(0)
      StartUpLog.logHour = dateArr(1).split(":")(0)
      StartUpLog.logHourMinute = dateArr(1)
      StartUpLog
    }


    val filteredDStream = StartUpLogStream.transform { rdd =>
      //driver  周期性执行
      //利用redis进行去重过滤
      val curdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedis: Jedis = RedisUtil.getJedisClient()
      val key = "dau" + curdate
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC = ssc.sparkContext.broadcast(dauSet)
      val filteredRDD = rdd.filter { StartUpLog =>
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(StartUpLog.mid)
      }
      filteredRDD
    }

    //去重思路,把相同的mid的数据分成一组，每组取第一个
    val groupByMidDStream: DStream[(String, Iterable[StartUpLog])] = filteredDStream.map(StartUpLog=>(StartUpLog.mid,StartUpLog)).groupByKey()

    val distinctDStream = groupByMidDStream.flatMap {
      case (mid, startUpLogItr) =>
        startUpLogItr.take(1)
    }


    //保存到redis中
    distinctDStream.foreachRDD{rdd=>
      
      //redis type set
      // key dau:2019-06-03  value:mids
      rdd.foreachPartition{startUpLogItr=>
        val jedis:Jedis = RedisUtil.getJedisClient
        val list = startUpLogItr.toList
        for(startUpLog <- list){
          val key = "dau:" + startUpLog.logDate
          val value = startUpLog.mid
          jedis.sadd(key,value)
        }

        if(list.size > 0){
          MyESUtil.indexBulk(GmailConstants.ES_INDEX_DAU,list)
        }
        jedis.close()
      }


      
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
