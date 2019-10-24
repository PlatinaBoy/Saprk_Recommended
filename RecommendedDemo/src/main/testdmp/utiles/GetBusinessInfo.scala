package main.testdmp.utiles

import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import java.security.NoSuchAlgorithmException

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
/**
  * ClassName: GetBusinessInfo <br/>
  * Description:
  * 获取business信息
  * * 1.校验sn计算方法
  * * 2.访问百度api获取json数据
  * * 3.解析json <br/>
  * date: 2019/4/18 16:46<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object GetBusinessInfo {

  def getBussinessInfo(lat:String,long:String) ={
    //获得url语句
    val url = getURL(lat,long)
    //创建http客户端
    val client = new HttpClient()
    //创建访问的方法
    val method = new GetMethod(url)
    //执行方法
    val status: Int = client.executeMethod(method)
    //判断status
    if (status == 200){
      val responseStr = method.getResponseBodyAsString
      parseJson(responseStr)
    }else{
      " "
    }
  }

  //解析json获取business信息
  private def parseJson(responseStr:String) ={
    var busInfo = new StringBuffer()
    if (StringUtils.isNotEmpty(responseStr)){
      //解析字符串成json数据
      val nObject: JSONObject = JSON.parseObject(responseStr)
      //获取status
      val status = nObject.getInteger("status")
      //判断status
      if (status == 0){
        //获取result数据转成json格式
        val jSONObject = nObject.getJSONObject("result")
        //获取business
        val business = jSONObject.getString("business")
        busInfo.append(business)
        //判断businfo是否为空
        if (StringUtils.isEmpty(busInfo.toString)){
          //获取json数组
          val jSONArray = jSONObject.getJSONArray("pois")
          //遍历循环
          for (i <- 0 until jSONArray.size()){
            //获取jsonArray里的每一个元素
            val jsonObj = jSONArray.getJSONObject(i)
            //获取tag
            val tag = jsonObj.getString("tag")
            //判断tag是否为空
            if (StringUtils.isNotEmpty(tag)){
              busInfo.append(tag)
            }
          }
        }
      }
    }
    busInfo.toString
  }

  @throws[UnsupportedEncodingException]
  @throws[NoSuchAlgorithmException]
  private def getURL(lat:String,long:String) = {
    // 计算sn跟参数对出现顺序有关，
    // get请求请使用LinkedHashMap保存<key,value>，
    // 该方法根据key的插入顺序排序；
    // post请使用TreeMap保存<key,value>，
    // 该方法会自动将key按照字母a-z顺序排序。
    // 所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，
    // 但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。
    // 以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，
    // paramsMap中先放入address，再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
    val paramsMap = new mutable.LinkedHashMap[String,String]
    paramsMap.put("location", lat+","+long)
    paramsMap.put("output", "json")
    paramsMap.put("pois", "1")
    paramsMap.put("ak", "IR73kvcPUYH3wBwRwZWez4T6vO9f2nAX")
    // 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
    val paramsStr = toQueryString(paramsMap)
    // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
    val wholeStr = new String("/geocoder/v2/?" + paramsStr + "OulQGjgHCTzmWdP1RZF5lceisc9b9ioA")
    // 对上面wholeStr再作utf8编码
    val tempStr = URLEncoder.encode(wholeStr, "UTF-8")
    // 调用下面的MD5方法得到最后的sn签名7de5a22212ffaa9e326444c75a58f9a0
    val sn = MD5(tempStr)
    "http://api.map.baidu.com/geocoder/v2/?"+paramsStr+"&sn="+sn
  }

  // 对Map内所有value作utf8编码，拼接返回结果
  @throws[UnsupportedEncodingException]
  private def toQueryString(data: mutable.LinkedHashMap[String,String]): String = {
    val queryString = new StringBuffer
    import scala.collection.JavaConversions._
    for (pair <- data.entrySet) {
      queryString.append(pair.getKey + "=")
      queryString.append(URLEncoder.encode(pair.getValue, "UTF-8") + "&")
    }
    if (queryString.length > 0) queryString.deleteCharAt(queryString.length - 1)
    queryString.toString
  }

  // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
  private def MD5(md5: String): String = {
    try {
      val md = java.security.MessageDigest.getInstance("MD5")
      val array = md.digest(md5.getBytes)
      val sb = new StringBuffer
      var i = 0
      while ( {
        i < array.length
      }) {
        sb.append(Integer.toHexString((array(i) & 0xFF) | 0x100).substring(1, 3))

        {
          i += 1; i
        }
      }
      return sb.toString
    } catch {
      case e: NoSuchAlgorithmException =>

    }
    null
  }


}
