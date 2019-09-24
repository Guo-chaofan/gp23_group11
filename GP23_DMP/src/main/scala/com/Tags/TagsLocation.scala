package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
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
object TagsLocation extends Tag{
  override def makTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val province: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(province)){
      list:+=("ZP"+province,1)
    }
    if(StringUtils.isNotBlank(city)){
      list:+=("ZC"+city,1)
    }
    list
  }
}
