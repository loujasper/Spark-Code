package com.bigdata.spark.Streaming.practice

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming2_kafka {

  def main(args: Array[String]): Unit = {

//    SparkStreaming最少要两个核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))


//  3.定义Kafka参数 //    kafka配置信息
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",  //集群配置
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",                                                //消费者组为单位来消费数据
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",           //key反序列化
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")         //value反序列化  为了提高数据传输效率，将数据进行序列化


//    使用sparkStreaming读取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](   //创建一个直连流的方法
        ssc, //环境对象
        LocationStrategies.PreferConsistent,   //
        ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara)    //主题名称Set("atguigu")
      )

//    TODO
      val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())




//    TODO 启动采集器
    ssc.start()

    for(i <- 1 to 10){
      val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString))
//      queue.enqueue(rdd)
      Thread.sleep(1000)
    }

//    TODO 等待采集器结束
    ssc.awaitTermination()






  }
}
