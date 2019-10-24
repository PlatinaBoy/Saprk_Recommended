package main.My

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * ClassName: ALS <br/>
  * Description: <br/>
  * date: 2019/7/17 9:45<br/>
  * 协同过滤推荐
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object ALS {
  def main(args: Array[String]): Unit = {
    //hive
    val HIVE_METASTORE_URIS = "thrift://hive-host-01:9083,thrift://hive-host-02:9083"
    System.setProperty("hive.metastore", HIVE_METASTORE_URIS)
    //sparksession
    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()
    //to be RDD
    val ratingDataOrc = sparkSession.sql("select userid,projectid,rate,timestame  from mite8.mite_ratings limit 50000")
    //装载样本评分数据，最后一列Timestamp去除10余数作为key，Rating为值，即(Int, Ratings)
    //输出的结果是一个key-value集合，其中key为时间取余，value是Rating对象
    val ratings: RDD[(Long, Rating)] = ratingDataOrc.rdd.map(f =>
      (java.lang.Long.parseLong(f.get(3).toString) % 10,
        Rating(java.lang.Integer.parseInt(f.get(0).toString),
          java.lang.Integer.parseInt(f.get(1).toString),
          f.get(2).asInstanceOf[java.math.BigDecimal].doubleValue())))
    ratings

    //筛选用户评分数据集，并将该用户集作为推荐用户对象
    val personalRatingsData: RDD[Rating] = ratingDataOrc.where("userid=1").rdd.map {
      f =>
        Rating(java.lang.Integer.parseInt(f.get(0).toString),
          java.lang.Integer.parseInt(f.get(1).toString),
          f.get(2).asInstanceOf[java.math.BigDecimal].doubleValue())
    }
    personalRatingsData

    //装载目录对照表（projectid，title）
    val projects = sparkSession.sql("select movieid,title from mite8.mite_movies").rdd.map {
      f =>
        (java.lang.Integer.parseInt(f.get(0).toString), f.get(1).toString)
    }
    projects
    System.out.println("=================001 GET DATA OK===========================")
    //统计有用户数量和数量以及用户对的评分数目
    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numProjests = ratings.map(_._2.product).distinct().count()

    println("==================样本数量===================")
    println("NumRatings: [" + numRatings + "]")
    println("NumUsers:   [" + numUsers + "]")
    println("numProjests:  [" + numProjests + "]")


    //将样本评分表以Key值切分成3个部分,并且数据在计算的过程中会多次用到，所以存入cache
    //-训练(60%，并加入用户评分)
    //-校验(20%)
    //-测试(20%)
    val numPartions=4
    //通过key(10的余数，均衡分布，所以x._1 < 6基本能够切分出大约60%的数据量)
    val training: RDD[Rating] = ratings.filter(x => x._1 < 6).values
      .union(personalRatingsData).repartition(numPartions).persist()
    training
    val validation: RDD[Rating] = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartions)().persist()
    val test: RDD[Rating] = ratings.filter(x=>x._1>8).values.persist()
    //统计各部分的量
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()
    //打印统计信息
    println("==================样本划分===================")
    println("NumTraining:     [" + numTraining + "]")
    println("NumValidation:   [" + numValidation + "]")
    println("NumTest:         [" + numTest + "]")

    //训练集中训练模型，在检验集中验证，训练最佳模型
    val ranks = List(5, 8, 12, 15)
    val lambdas = List(0.1, 0.5, 5)
    val numIters = List(8, 10, 20)
    //最佳模型变量
    var bestModel:Option[MatrixFactorizationModel]=None
    val bestValidationRmse = Double.MaxValue





  }


}
