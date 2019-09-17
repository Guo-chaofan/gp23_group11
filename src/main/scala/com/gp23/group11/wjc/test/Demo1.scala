package com.gp23.group11.wjc.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  /**
    * 进行数据格式转换
    * */
  def main(args: Array[String]): Unit = {
    if (args.length != 2){
      println("目录不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    val conf = new SparkConf().setAppName("dd1")
     .setMaster("local[*]").set("","") //序列化级别
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //设置压缩方式
    sqlContext.setConf("","")
    //处理数据
    val lines = sc.textFile(inputPath)
    lines.map(t=> t.split(",",-1))
      .filter(t=>t.length>=85)
      .map(arr=>{Row()})
  }

}
