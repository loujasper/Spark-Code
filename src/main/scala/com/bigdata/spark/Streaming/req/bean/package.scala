package com.bigdata.spark.Streaming.req

package object bean {

  case class Ad_Click_log( ts:String,area : String,city : String,userid : String,adid : String )
}
