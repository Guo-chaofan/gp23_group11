package com.etl

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月19日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object selectDemo {
  def main(args: Array[String]): Unit = {
    val session=SparkSession
      .builder()
      .appName("select")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val Array(inputPath,outputPath)=args
    val df: DataFrame = session.read.parquet(inputPath)
    df.createTempView("table")
    val df2: DataFrame = session.sql("select networkmannerid,networkmannername,count(*) ct from table group by networkmannerid,networkmannername ")
    df2.write.partitionBy("networkmannerid","networkmannername")json(outputPath)
  }

}
