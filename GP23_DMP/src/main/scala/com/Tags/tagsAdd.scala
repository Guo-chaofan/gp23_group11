package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月21日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object tagsAdd extends Tag{
  override def makTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    //获取数据类型
    val row= args(0).asInstanceOf[Row]
    //获取广告类型和广告名称
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case t if t>9 =>list:+=("LC"+t,1)
      case t if t>0 && t<=9 =>list:+=("LC0"+t,1)
    }
    val name= row.getAs[String]("adspacetypename")
    if(StringUtils.isBlank(name)){
      list:+=("LN"+name,1)
    }
    list

  }
}
