package com.bigdata.spark.Streaming.req.service

import com.bigdata.spark.Streaming.req.dao.MockDataDao
import com.bigdata.summer.framework.core.TService


class MockDataService extends TService{

  private val mockDataDao = new MockDataDao

  override def analysis() = {


//    TODO 生成数据
   // import mockDataDao._
   val datas = mockDataDao.genMockData()

//    TODO 向kafka发送数据
    mockDataDao.writeToKafka(datas)
  }
}
