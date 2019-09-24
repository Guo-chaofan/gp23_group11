package com.etl

import com.util.HttpUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月21日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object testdemoooo {
  def main(args: Array[String]): Unit = {
    val session=SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()
    val arr=Array("https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=16b1b47347b4f7b9facf4f37c2bc7c1d&extensions=all")
    val unit: RDD[String] = session.sparkContext.makeRDD(arr)
    unit.map(t=>{
      HttpUtils.get(t)
    })
      .foreach(println)
  }

}
