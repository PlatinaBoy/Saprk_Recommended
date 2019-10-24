package main.shuashuaspark_ALS.portrait

import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.util.control.Breaks
import scala.collection.JavaConversions._

/**
  * ClassName: PortraitApp <br/>
  * Description: 计算用户画像标签<br/>
  * date: 2019/4/23 15:05<br/>
  * @author PlatinaBoy<br/>
  * @version Test
  */
object PortraitApp {
  def main(args: Array[String]): Unit = {
    //设置hive访问host以及端口
    //    val HIVE_METASTORE_URIS = ""
    //注意：实际使用的时候，替换成服务器集群中，hive的host以及访问端口
    val HIVE_METASTORE_URIS = " "
    System.setProperty("hive.metastore.uris", HIVE_METASTORE_URIS)

    //构建一个通用的sparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("portrait_m")
      .enableHiveSupport()
      .getOrCreate()

    //DataFrame格式化申明
    val schemaString = "shopid tag"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    //获取rating评分数据集
    val ratingData = sparkSession.sql("select userid,shopid,rate  from \\table_name\\")

    //获取tags数据
    val tagsData = sparkSession.sql("select shopid,tag from \\table_name\\").rdd

    System.out.println("=================001===========================")

    //进行tags标签的处理,包括分词，去除停用词等等
    val tagsStandardize = tagsData.map {
      f =>
        val shopid = f.get(0)
        //进行逻辑判断，size>3的进行标准化处理
        val tag = if (f.get(1).toString.split(" ").size <= 3) {
          f.get(1)
        } else {
          //进行主题词抽取(能屏蔽掉停用词)
          HanLP.extractKeyword(f.get(1).toString, 20).toSet.mkString(" ")
        }
        (shopid, tag)
    }

    System.out.println("=================002===========================")

    //进行相似tag合并操作，最终返回依然是(shopid,tag)集合，但tag会做预处理
    val tagsStandardizeTmp = tagsStandardize.collect()
    val tagsSimi = tagsStandardize.map {
      f =>
        var retTag = f._2
        if (f._2.toString.split(" ").size == 1) {
          var simiTmp = ""

          val tagsTmpStand = tagsStandardizeTmp
            .filter(_._2.toString.split(" ").size != 1)
            .filter(f._2.toString.size < _._2.toString.size)
            .sortBy(_._2.toString.size)

          var x = 0
          val loop = new Breaks

          tagsTmpStand.map {
            tagTmp =>
              val flag = getEditSize(f._2.toString, tagTmp._2.toString)
              if (flag == 1) {
                retTag = tagTmp._2
                loop.break()
              }
          }

          (f._1, retTag)
        } else {
          f
        }
    }.map(f => Row(f._1, f._2.toString.trim))

    System.out.println("============================================")

    //先将预处理之后的SHOP-tag数据进行dataframe
    //    val schemaString = "shopid tag"
    //    val schema = StructType(schemaString.split(" ")
    //    .map(fieldName=>StructField(fieldName,StringType,true)))
    val tagsSimiDataFrame = sparkSession.createDataFrame(tagsSimi, schema)

    //对rating(userid,shopid,rate)，tags(shopid,tag)进行join，以shopid关联
    //join步骤：将(userId, shopId, rate)与(shopId, tag)按照shopId字段进行连接
    val tagRateDataFrame = ratingData.join(tagsSimiDataFrame, ratingData("shopid") === tagsSimiDataFrame("shopid"), "inner")
      .select("userid", "tag", "rate")

    System.out.println("==========================================")

    //reduce步骤：将(userId, tag, rate)中(userId, tag)相同的分数rate相加
    val tagSumRateDataFrame = tagRateDataFrame.groupBy("userid", "tag").sum("rate")

    System.out.println("============================================")

    //最终数据重新按userid升序,tag的rate的降序排序
    val userTags = tagSumRateDataFrame.orderBy(tagSumRateDataFrame("userid"), tagSumRateDataFrame("sum(rate)").desc)

    System.out.println("==============================================")

    //将结果存入hive
    //    val userTagTmpTableName = ""
    //    val userTagTableName = ""
    //    userTags.registerTempTable(userTagTmpTableName)
    //    sparkSession.sql("insert into table " + userTagTableName + " select * from " + userTagTmpTableName)
    // 将数据存入数据库中mysql redis 等等

    System.out.println("===============================================")

  }

  //合并tag，合并原则：长度=1(单个词)；前缀相似度>=2/7，
  def getEditSize(str1: String, str2: String): Int = {
    if (str2.size > str1.size) {
      0
    } else {
      //计数器
      var count = 0
      val loop = new Breaks
      //以较短的str2进行遍历，并逐个比较
      val lengthStr2 = str2.getBytes().length
      var i = 0
      for (i <- 1 to lengthStr2) {
        if (str2.getBytes()(i) == str1.getBytes()(i)) {
          //逐个匹配字节，相等则计数器+1
          count += 1
        } else {
          //一旦出现前缀不一致则中断循环，开始计算重叠度
          loop.break()
        }
      }

      //计算重叠度,当前缀重叠度大于等于2/7时，进行合并
      if (count.asInstanceOf[Double] / str1.getBytes().size.asInstanceOf[Double] >= (1 - 0.286)) {
        1
      } else {
        0
      }
    }
  }

}
