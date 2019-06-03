package glx.cn.sh
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.TaskContext
import scala.util.matching.Regex
import redis.clients.jedis.Jedis

object scalaJob{
  def main(args: Array[String]) {
    val brokers = "172.17.0.59:9092"
    val group = "test-consumer-group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    ////////////////////////////////////////////////////////////////////////
    //                           简单获取kafka数据                        //
    val topics = Array("MetisTest")
    val conf = new SparkConf().setMaster("local[2]").setAppName("offset demo")
//    conf.set("spark.redis.host","127.0.0.1")
//    conf.set("spark.redis.port","6379")
    val ssc = new StreamingContext(conf, Seconds(5))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    /////////////////////////////////////////////////////////////////////////
    
    ////////////////////////////////////////////////////////////////////////////////
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        val jedis = new Jedis("localhost", 6379);
        jedis.lpush("fromOffset",s"${o.fromOffset}")
        jedis.close()
        //////////////////////////////////////////////////////////////
        //                      逐条打印数据                        //
        //while(iter.hasNext){
        //  println(s"iter value ${iter.next.value}")
        //}
        //////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////
        ////               逐条打印复合正则条件的数据             ////
        ////        只打印城市是 Shanghai并系统是 iOs的数据       ////
        while(iter.hasNext){
          val iterValue = iter.next.value
          val strParttern : Regex = "\"Shanghai\".+\"iOs\"".r 
          strParttern.findFirstMatchIn(iterValue) match {
            case Some(_) => println(iterValue)
            case None => 
          }
        }
        //////////////////////////////////////////////////////////////
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }



}
