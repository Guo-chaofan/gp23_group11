package com.Tags

import com.util.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月23日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object TagsKword extends Tag{
  override def makTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val kword= args(1).asInstanceOf[Broadcast[collection.Map[String,Int]]]
    row.getAs[String]("keywords").split("\\|")
      .filter(word=>word.length>=3&&word.length<=8 && !kword.value.contains(word))
        .foreach(word=>list:+=(("K"+word),1))
    list
  }
}
