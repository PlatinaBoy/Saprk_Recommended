
import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.util.control.Breaks

/**
  * ClassName: RecommendOne <br/>
  * Description: 基于项目属性的推荐<br/>
  * date: 2019/4/26 12:42<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object RecommendOne {
  def main(args: Array[String]): Unit = {

    //设置hive访问host以及端口  hadoop9
    //注意：实际使用的时候，替换成自己服务器集群中，hive的host以及访问端口
    val HIVE_METASTORE_URIS = " "
    System.setProperty(" ", HIVE_METASTORE_URIS)

    //构建一个通用的sparkSession
    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()

    //DataFrame初始化
    val schemaString = "projectid tag"
    val schema = StructType(schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, true)))

    //获取评分数据集(projectid,project_rate)
    // 这边可以通过sql，hsql 在数据库中完成，并且可以完成对字段的加权最后表结构
    // 待定。。。。此处仅做参考这张应该是张新表
    val movieAvgRate = sparkSession.sql("select id,project_rate  from xxxxx group by projectid")
      .rdd.map {
      f =>
        (f.get(0), f.get(1))
    }

    //获取项目属性数据，也是做参考，数据表名ay_b_gm2c_project_info
    //何字段作为属性数据商榷，此处做参考
    val projectData = sparkSession
      .sql("select id,project_type,project_name form ay_b_gm2c_project_info").rdd

    //获取项目tages数据
    //    //同样应该是张新表
    val tagsData = sparkSession
      .sql("").rdd


    //获取客户字段信息
    val somepeople = sparkSession.sql("").rdd

    System.out.println("==================获取数据==========================")

    //进行tags标签的处理,包括分词，去除停用词等等根据。
    val tagsStandardize = tagsData.map {
      f =>
        val projectId = f.get(0)
        //进行逻辑判断，eg:size>3的进行标准化处理
        val tag = if (f.get(1).toString.split(" ", -1).size <= 3) {
          f.get(1)
        } else {
          //进行主题词抽取(能屏蔽掉停用词)
          HanLP.extractKeyword(f.get(1).toString, 20).toSet.mkString(" ")
        }
        (projectId, tag)
    }


    System.out.println("=====================HanLP==========================")


    //进行相似tag的合并,最终返回依然是(projectid,tag)集合，但tag会做预处理
    val tagsStandardizeTmp = tagsStandardize.collect()
    val tagsSimi = tagsStandardize.map {
      f =>
        var retTag = f._2

    }
    System.out.println("=====================预处理tag==========================")







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


}