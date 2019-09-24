package com.util

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client. HttpClients
import org.apache.http.util.EntityUtils


/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月21日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object HttpUtils {
  def get (url:String):String={
    val client= HttpClients.createDefault()
    val httpGet=new HttpGet(url)
    //发送请求
    val value= client.execute(httpGet)
    EntityUtils.toString(value.getEntity,"UTF-8")
  }

}
