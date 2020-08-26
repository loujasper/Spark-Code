package com.bigdata.spark.Streaming.practice

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming5_window {

  def main(args: Array[String]): Unit = {

//    SparkStreaming最少要两个核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.sparkContext.setCheckpointDir("cp")   //里计算的中间结果需要报错到检查点的位置中。所以需要设定检查点路径。

//    从socket拿数据
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

//    TODO 窗口
    val ints = Array(1,2,3,4,5,6,7,8)

//    ints.sliding(3)


    val wordDS: DStream[String] = ds.flatMap(_.split(" "))

    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_, 1))

//    TODO 将多个采集周期作为计算整体，比如将三个采集周期一起统计计算
    //val result: DStream[(String, Int)] = wordToOneDS.window(Seconds(9)).reduceByKey(_+_)  //3个采集周期 * 3
    //Seconds(6)窗口的尖酸周期等同于窗口滑动的步长
//    窗口的范围和滑动的步长都应该是采集周期（3s）的整数倍。
    val result: DStream[(String, Int)] = wordToOneDS.window(Seconds(9),Seconds(6)).reduceByKey(_+_)  //3个采集周期 * 3

    /**
      * 窗口的范围是采集周期的整数倍
      * 默认滑块的复读（步长）为一个采集周期
      *
      *
      *
      */

    result.print()



//    TODO 启动采集器
    ssc.start()

//    TODO 等待采集器结束
    ssc.awaitTermination()






  }
}
