package com.test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月22日
  *
  * @author 杨伟华
  * @version : 1.0
 json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
   1、按照pois，分类businessarea，并统计每个businessarea的总数。
   2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */

object testdemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    val txt: RDD[String] = session.sparkContext.textFile("F:\\QQQQQ\\json.txt")
    import session.implicits._
    txt.map(x =>getTags(x)).filter(_._1!="[]").collect().toBuffer.foreach(println)


  }
  def getTags(string: String):(String,Int)={
    val json= JSON.parseObject(string)
    val i = json.getIntValue("status")
    if(i==0) return ("无",1)
    val regeocodeJSON = json.getJSONObject("regeocode")
    if(regeocodeJSON==null||regeocodeJSON.keySet().isEmpty) return ("无",1)
    val poisArray=regeocodeJSON.getJSONArray("pois")
    if(poisArray==null||poisArray.isEmpty) return("无",1)
    val buffer = collection.mutable.ListBuffer[(String, Int)]()
    for (item <- poisArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        if (item.asInstanceOf[JSONObject].getString("businessarea").isEmpty||item.asInstanceOf[JSONObject].getString("businessarea")==null) {
          buffer.append(("无", 1))
        }
        else {
        buffer.append((item.asInstanceOf[JSONObject].getString("businessarea"),1))
        }
      }
    }
    val stringToTuples: Map[String, ListBuffer[(String, Int)]] = buffer.groupBy(_._1)
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(_.foldLeft[Int](0)(_ + _._2))
    val array: Array[(String, Int)] = stringToInt.toArray
    (array(0)._1, array(0)._2)

  }
  def getTypes(string: String): Array[(String, Int)] = {
    //对JSON解析并过滤不包含目标数据的JSON
    val json = JSON.parseObject(string)
    if (json.getIntValue("status") == 0) return Array(("无",1))
    val regeocodeJSON = json.getJSONObject("regeocode")
    if (regeocodeJSON == null || regeocodeJSON.keySet().isEmpty) return Array(("无",1))
    val poisArray = regeocodeJSON.getJSONArray("pois")
    if (poisArray == null || poisArray.isEmpty) return Array(("无",1))
    val buffer = collection.mutable.ListBuffer[Array[(String, Int)]]()
    for (item <- poisArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        if ((item.asInstanceOf[JSONObject].getString("type").isEmpty)) {
          buffer.append(Array(("无", 1)))
        } else {
          val tuples: Array[(String, Int)] = item.asInstanceOf[JSONObject].getString("type").split(";")
            .map((_, 1))
          buffer.append(tuples)
        }
      }
    }
    //求该条JSON对应的type的数量
    val stringToTuples: Map[String, ListBuffer[(String, Int)]] = buffer.flatten
      .groupBy(_._1)
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(_.foldLeft(0)(_+_._2))
    val array: Array[(String, Int)] = stringToInt.toArray
    array
  }
}

