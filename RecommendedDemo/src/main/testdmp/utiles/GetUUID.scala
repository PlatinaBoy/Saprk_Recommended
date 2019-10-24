package main.testdmp.utiles

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * ClassName: GetUUID <br/>
  * Description: <br/>
  * date: 2019/4/18 16:45<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object GetUUID {
  val filtedUUID = """
                     |imei != '' or mac != '' or idfa != '' or openudid != '' or  androidid != '' or
                     |imeimd5 != '' or macmd5 != '' or idfamd5 != '' or openudidmd5 != '' or  androididmd5 != '' or
                     |imeisha1 != '' or macsha1 != '' or idfasha1 != '' or openudidsha1 != '' or  androididsha1 != ''
                   """.stripMargin
  def getUUID(row:Row) ={
    row match {
      case v if (StringUtils.isNotEmpty(v.getAs[String]("imeimd5"))) => "imeimd5:" + v.getAs[String]("imeimd5")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("macmd5"))) => "macmd5:" + v.getAs[String]("macmd5")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("idfamd5"))) => "idfamd5:" + v.getAs[String]("idfamd5")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("openudidmd5"))) => "openudidmd5:" + v.getAs[String]("openudidmd5")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("androididmd5"))) => "androididmd5:" + v.getAs[String]("androididmd5")

      case v if (StringUtils.isNotEmpty(v.getAs[String]("imeisha1"))) => "imeisha1:" + v.getAs[String]("imeisha1")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("macsha1"))) => "macsha1:" + v.getAs[String]("macsha1")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("idfasha1"))) => "idfasha1:" + v.getAs[String]("idfasha1")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("openudidsha1"))) => "openudidsha1:" + v.getAs[String]("openudidsha1")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("androididsha1"))) => "androididsha1:" + v.getAs[String]("androididsha1")

      case v if (StringUtils.isNotEmpty(v.getAs[String]("imei"))) => "imei:" + v.getAs[String]("imei")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("mac"))) => "mac:" + v.getAs[String]("mac")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("idfa"))) => "idfa:" + v.getAs[String]("idfa")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("openudid"))) => "openudid:" + v.getAs[String]("openudid")
      case v if (StringUtils.isNotEmpty(v.getAs[String]("androidid"))) => "androidid:" + v.getAs[String]("androidid")
    }
  }

}
