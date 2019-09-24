package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
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
object TagsApp extends Tag{
  override def makTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row= args(0).asInstanceOf[Row]
    val broad= args(1).asInstanceOf[Broadcast[collection.Map[String,Int]]]
    //获得appname和appid
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else{
      list:+=("APP"+broad.value.getOrElse(appid,"其他"),1)
    }
    list

  }
}
