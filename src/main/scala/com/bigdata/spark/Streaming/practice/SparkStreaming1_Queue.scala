package com.bigdata.spark.Streaming.practice

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object SparkStreaming1_Queue {

  def main(args: Array[String]): Unit = {

//    SparkStreaming最少要两个核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val queue = new mutable.Queue[RDD[String]]()

    val queueDS: InputDStream[String] = ssc.queueStream(queue)

    queueDS.print()


//    TODO 启动采集器
    ssc.start()

    for(i <- 1 to 10){
      val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString))
      queue.enqueue(rdd)
      Thread.sleep(1000)
    }

//    TODO 等待采集器结束
    ssc.awaitTermination()






  }
}
