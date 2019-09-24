package com.util

import java.lang

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import io.netty.handler.codec.http.HttpUtil

/**
  * Description：xxxx<br/>
  * Copyright (c) ， 2019， Yang <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年09月21日
  *
  * @author 杨伟华
  * @version : 1.0
  */
object AmapUtils {
  def getBusinessFromAmap(long:Double,lag:Double):String={
    val location=long+","+lag
    //获取url
    val url="https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=16b1b47347b4f7b9facf4f37c2bc7c1d"
    //调用Http接口发送请求
    val jsonstr = HttpUtils.get(url)
    //获得json对象
    val jsonObeject: JSONObject = JSON.parseObject(jsonstr)
    //判断当前状态是否为1
    val i = jsonObeject.getIntValue("status")
    if(i==0)return ""
    // 如果不为空
    val jSONObject2 = jsonObeject.getJSONObject("regeocode")
    //拿到地址元素列表对象
    val jsonObject2=jSONObject2.getJSONObject("addressComponent")
    //判断
    if(jsonObject2 == null) return ""
    //获得商圈数组
    val jsonArray = jsonObject2.getJSONArray("businessAreas")
    if(jsonArray == null) return ""
    //定义集合取值
    val result =collection.mutable.ListBuffer[String]()
    //循环数组
    for(item <- jsonArray.toArray()){
      //判断是否属于json串
      if(item.isInstanceOf[JSONObject]){
        //将item转换成json对象
        val json=item.asInstanceOf[JSONObject]
        val name= json.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }

}
