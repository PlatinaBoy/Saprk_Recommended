package main.testdmp.utiles

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import testdmp.confighelp.ConfigHelper

/**
  * ClassName: Business2Mysql <br/>
  * Description: 商圈知识库建立在mysql<br/>
  * date: 2019/4/18 16:54<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object Business2Mysql {
  def main(args: Array[String]): Unit = {
    //sparkconf
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
      .set("spark.serializer", ConfigHelper.serializer)
    //sparkcontext
    val sc = new SparkContext(conf)
    //sqlcontext
    val sQLContext = new SQLContext(sc)
    val frame = sQLContext.read.parquet("")
    import sQLContext.implicits._
    val result = frame.map(row => {
      val long = row.getAs[String]("long")
      val lat = row.getAs[String]("lat")
      //调取百度api
      val businfo: String = GetBusinessInfo.getBussinessInfo(lat, long)
      val geohash = GeoHash.withCharacterPrecision(TurnType.toDouble(lat), TurnType
        .toDouble(long), 12).toBase32
      (geohash, businfo)


    }).filter(x => StringUtils.isNotEmpty(x._2))
    //存入数据库
    val properties = new Properties()


    properties.setProperty("driver", ConfigHelper.driver)
    properties.setProperty("user", ConfigHelper.user)
    properties.setProperty("password", ConfigHelper.password)
    result.map(x => (x._1, x._2))
      .toDF("geoHash", "bussiness")
      .write.jdbc(ConfigHelper.url, "bussinessinfo", properties)
    sc.stop()


  }

}
