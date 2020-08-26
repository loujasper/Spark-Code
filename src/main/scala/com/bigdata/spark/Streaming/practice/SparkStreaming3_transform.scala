package com.bigdata.spark.Streaming.practice

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming3_transform {

  def main(args: Array[String]): Unit = {

//    SparkStreaming最少要两个核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)


    //code1 driver端只执行一次  RDD之外的代码只执行一次
    val newDS: DStream[String] = ds.transform( //transform方法内是对RDD的操作  RDD[T] => RDD[U]
      rdd => {
//        code2 driver端执行N次 RDD内部的代码调用一次RDD就执行一次
        rdd.map(
          data => {
            //code3 Excutor 端执行N次
            data * 2
          }
        )
      }

    )
//    code1 driver端执行一次
    val newDS1 = ds.map(  //map算子之内是没有RDD的 是对数据的操作
      data => {
//        code2 excutor执行N次
        data * 2
      }
    )



//    TODO 启动采集器
    ssc.start()

//    TODO 等待采集器结束
    ssc.awaitTermination()






  }
}
