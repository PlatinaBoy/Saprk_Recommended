package main.testdmp.utiles

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer
/**
  * ClassName: GetAllUserid <br/>
  * Description: <br/>
  * date: 2019/4/15 17:54<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object GetAllUserid {
  def getAllUserid(row:Row): Unit ={
    var list = new ListBuffer[String]()
    if (StringUtils.isNotEmpty(row.getAs[String]("imeimd5")))list.append("imeimd5:" + row.getAs[String]("imeimd5"))
    if (StringUtils.isNotEmpty(row.getAs[String]("macmd5"))) list.append("macmd5:" + row.getAs[String]("macmd5"))
    if (StringUtils.isNotEmpty(row.getAs[String]("idfamd5"))) list.append("idfamd5:" + row.getAs[String]("idfamd5"))
    if (StringUtils.isNotEmpty(row.getAs[String]("openudidmd5"))) list.append("openudidmd5:" + row.getAs[String]("openudidmd5"))
    if (StringUtils.isNotEmpty(row.getAs[String]("androididmd5"))) list.append("androididmd5:" + row.getAs[String]("androididmd5"))

    if (StringUtils.isNotEmpty(row.getAs[String]("imeisha1"))) list.append("imeisha1:" + row.getAs[String]("imeisha1"))
    if (StringUtils.isNotEmpty(row.getAs[String]("macsha1"))) list.append("macsha1:" + row.getAs[String]("macsha1"))
    if (StringUtils.isNotEmpty(row.getAs[String]("idfasha1"))) list.append("idfasha1:" + row.getAs[String]("idfasha1"))
    if (StringUtils.isNotEmpty(row.getAs[String]("openudidsha1"))) list.append("openudidsha1:" + row.getAs[String]("openudidsha1"))
    if (StringUtils.isNotEmpty(row.getAs[String]("androididsha1"))) list.append("androididsha1:" + row.getAs[String]("androididsha1"))

    if (StringUtils.isNotEmpty(row.getAs[String]("imei"))) list.append("imei:" + row.getAs[String]("imei"))
    if (StringUtils.isNotEmpty(row.getAs[String]("mac"))) list.append("mac:" + row.getAs[String]("mac"))
    if (StringUtils.isNotEmpty(row.getAs[String]("idfa"))) list.append("idfa:" + row.getAs[String]("idfa"))
    if (StringUtils.isNotEmpty(row.getAs[String]("openudid"))) list.append("openudid:" + row.getAs[String]("openudid"))
    if (StringUtils.isNotEmpty(row.getAs[String]("androidid"))) list.append("androidid:" + row.getAs[String]("androidid"))
    list


  }



}
