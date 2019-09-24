package test

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object tagsUtils {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jsonRdd: RDD[String] = sc.textFile("F:\\QQQQQ\\json.txt")

    //  1ti
    //过滤掉空的没有商圈的
    jsonRdd.map(x =>getBusinessArea(x)).filter(_._1!="[]").collect().toBuffer.foreach(println)
    println("------")
    //2ti
    //将所有数据合并并求每个type的总数
    jsonRdd.map(x => getTypes(x))
      .flatMap(x => x)
      .reduceByKey(_+_)
      .collect().toBuffer.foreach(println)
  }

  def getBusinessArea(string: String): (String, Int) = {
    //对JSON解析并过滤不包含目标数据的JSON
    val json = JSON.parseObject(string)
    if (json.getIntValue("status") == 0) return ("无",1)
    val regeocodeJSON = json.getJSONObject("regeocode")
    if (regeocodeJSON == null || regeocodeJSON.keySet().isEmpty) return ("无",1)
    val poisArray = regeocodeJSON.getJSONArray("pois")
    if (poisArray == null || poisArray.isEmpty) return ("无",1)
    val buffer = collection.mutable.ListBuffer[(String, Int)]()
    for (item <- poisArray.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        if (item.asInstanceOf[JSONObject].getString("businessarea").isEmpty||item.asInstanceOf[JSONObject].getString("businessarea")==null) {
          buffer.append(("无", 1))
        } else {
          //将每个地点计数为1
          buffer.append((item.asInstanceOf[JSONObject].getString("businessarea"), 1))
        }
      }
    }
    //求每个商圈的各自总数
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
