package com.Tags

import com.typesafe.config.ConfigFactory
import com.util
import com.util.tagsUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签主类
  */
object tagsContext {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length!=3){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath,docs,stopwords,day)=args

    // 创建Spark上下文
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._
    // 调用HbaseAPI
    val load = ConfigFactory.load()
    // 获取表名
    val HbaseTableName = load.getString("HBASE.tableName")
    // 创建Hadoop任务
    val configuration = spark.sparkContext.hadoopConfiguration
    // 配置Hbase连接
    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
    // 获取connection连接
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbConn.getAdmin
    // 判断当前表是否被使用
    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
      println("当前表可用")
      // 创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
      // 创建列簇
      val hColumnDescriptor = new HColumnDescriptor("tags")
      // 将创建好的列簇加入表中
      tableDescriptor.addFamily(hColumnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbConn.close()
    }
    val conf = new JobConf(configuration)
    // 指定输出类型
    conf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出哪张表
    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)

    // 读取数据文件
    val df = spark.read.parquet(inputPath)
    //读取字典文件
    val docsRDD: collection.Map[String, String] = spark.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()

    //广播字典
    val broadValue: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docsRDD)

    //读取停用字典
    val stopwordsRDD: collection.Map[String, Int] = spark.sparkContext.textFile(stopwords).map((_,0)).collectAsMap()

    //广播字典
    val broadValues: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopwordsRDD)

    // 处理数据信息
    df.map(row=>{
      // 获取用户的唯一ID
      val userId = tagsUtils.getUserid(row)
      // 接下来标签 实现
      val adList = tagsAdd.makTags(row)
      // 商圈
      val businessList = BusinessTag.makTags(row)
      // 媒体标签
      val appList = TagsApp.makTags(row,broadValue)
      // 设备标签
      val devList = TagsDevices.makTags(row)
      // 地域标签
      val locList = TagsLocation.makTags(row)
      // 关键字标签
      val kwList = TagsKword.makTags(row,broadValues)
      (userId,adList++businessList++appList++devList++locList++kwList)
    })
      .rdd.reduceByKey((list1,list2)=>{
      (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    }).map{
      case (userId,userTags) =>{
        // 设置rowkey和列、列名
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(conf)

  }
}
