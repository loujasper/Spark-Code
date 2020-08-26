package com.bigdata.spark.Streaming.practice

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming_WordCount {

  def main(args: Array[String]): Unit = {

//    SparkStreaming最少要两个核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

//    TODO 从socket获取数据，按行获取
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

//    WrodCount
    val wordToSumDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wordToSumDS.print()


//    TODO 启动采集器
    ssc.start()

//    TODO 等待采集器结束
    ssc.awaitTermination()






  }
}
