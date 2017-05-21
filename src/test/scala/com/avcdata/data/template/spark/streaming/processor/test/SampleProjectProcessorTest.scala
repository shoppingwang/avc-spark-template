package com.avcdata.data.template.spark.streaming.processor.test

import com.avcdata.etl.streaming.launcher.StreamProcessLauncher
import org.junit.Test

/**
  * 示例工程处理器测试
  * <p/>
  * Author   : wangxp
  * <p/>
  * DateTime : 16/7/11 13:30
  */
class SampleProjectProcessorTest
{
  @Test
  def testProcess(): Unit =
  {
    val args = Array[String]("--process-class"
      , "com.avcdata.data.template.spark.streaming.processor.SampleProjectProcessor"
      , "--batch-duration-seconds"
      , "10"
      , "--param"
      , "kafka.read.topics=TimeHonoredBrandPosts"
      , "--param"
      , "kafka.zookeeper.hosts=zk-host1:2181,zk-host2:2181,zk-host3:2181"
      , "--param"
      , "data.common.db.connecturi=jdbc:mysql://host:3306/avc_data_common?useUnicode=true&characterEncoding=utf-8&useSSL=false"
      , "--param"
      , "data.common.db.username=user"
      , "--param"
      , "data.common.db.password=pass"
    )

    StreamProcessLauncher.main(args)
  }
}
