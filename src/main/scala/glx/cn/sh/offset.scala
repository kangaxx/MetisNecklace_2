package glx.cn.sh
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import kafka.serializer.{StringDecoder, DefaultDecoder}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.TaskContext
import scala.util.matching.Regex
import org.apache.kafka.common._

object offsetJob{
  def main(args: Array[String]) {
    val brokers = "172.17.0.59:9092"
    val group = "test-consumer-group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Iterable("MetisTest")
    val conf = new SparkConf().setMaster("local[2]").setAppName("offset demo")
    val ssc = new StreamingContext(conf, Seconds(5))
    var offsets: Map[TopicPartition, Long] = Map()
    val tp =new TopicPartition("MetisTest", 0)
    offsets += (tp -> 29868290L)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
    )
    /////////////////////////////////////////////////////////////////////////
    
    ////////////////////////////////////////////////////////////////////////////////
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange =offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        //////////////////////////////////////////////////////////////
        //                      逐条打印数据                        //
        while(iter.hasNext){
          println(s"iter value ${iter.next.value} ")
        }
        //////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////
        ////               逐条打印复合正则条件的数据             ////
        ////        只打印城市是 Shanghai并系统是 iOs的数据       ////
        ////while(iter.hasNext){
        ////  val iterValue = iter.next.value
        ////  val strParttern : Regex = "\"Shanghai\".+\"iOs\"".r 
        ////  strParttern.findFirstMatchIn(iterValue) match {
        ////    case Some(_) => println(iterValue)
        ////    case None => 
        ////  }
        ////}
        //////////////////////////////////////////////////////////////
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
