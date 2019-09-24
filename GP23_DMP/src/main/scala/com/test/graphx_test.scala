package com.test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月23日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object graphx_test {
  def main(args: Array[String]): Unit = {
    val session=SparkSession.builder().appName("graphx").master("local[*]").getOrCreate()

    //创建边和点的集合
    val vertexRDD = session.sparkContext.makeRDD(Seq(
      (1L,("小明",26)),
      (2L,("小红",30)),
      (6L,("小黑",33)),
      (9L,("小白",26)),
      (133L,("小黄",30)),
      (138L,("小蓝",33)),
      (158L,("小绿",26)),
      (16L,("小龙",30)),
      (44L,("小强",33)),
      (21L,("小胡",26)),
      (5L,("小狗",30)),
      (7L,("小熊",33))
    ))
    // 构造边的集合
    val edgeRDD = session.sparkContext.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(6L,133L,0),
      Edge(9L,133L,0),
      Edge(6L,138L,0),
      Edge(16L,138L,0),
      Edge(21L,138L,0),
      Edge(44L,138L,0),
      Edge(5L,158L,0),
      Edge(7L,158L,0)
    ))

    //构建图
    val graph = Graph(vertexRDD,edgeRDD)

    //去顶点
    val vertice: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //匹配数据
    vertice.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }.reduceByKey(_++_)
      .foreach(println)

  }

}
