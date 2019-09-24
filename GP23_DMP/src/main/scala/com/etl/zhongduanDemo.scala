package com.etl

import com.util.RtpUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月18日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object zhongduanDemo {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("参数错误，请重试")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    val session=SparkSession.builder()
      .master("local[*]")
      .appName("zhongduan")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = session.read.parquet(inputPath)
    df.rdd.map((row) => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 处理请求数
      val rptList = RtpUtils.ReqPt(requestmode, processnode)
      // 处理展示点击
      val clickList = RtpUtils.clickPt(requestmode, iseffective)
      // 处理广告
      val adList = RtpUtils.adPt(iseffective, isbilling, isbid, iswin, adordeerid, winprice, adpayment)
      // 所有指标
      val allList: List[Double] = rptList ++ clickList ++ adList
      //      (row.getAs[String]("ispname"), allList)
      //    }).reduceByKey((list1, list2) => {
      //      // list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      //      list1.zip(list2).map(t => t._1 + t._2)
      //    })
      //      .map(t => t._1 + "," + t._2.mkString(","))
      //
      //      .saveAsTextFile(outputPath)
      (row.getAs[Int]("devicetype"), allList)
    }).reduceByKey((x1,x2)=>{
      x1.zip(x2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)
  }

}
