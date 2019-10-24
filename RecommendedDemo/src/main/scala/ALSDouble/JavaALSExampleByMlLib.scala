package main.scala.ALSDouble

import java.io.{File, IOException}
import java.util
import java.util.Arrays

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.api.java.{JavaDoubleRDD, JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.Function
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.slf4j.{Logger, LoggerFactory}

/**
  * ClassName: JavaALSExampleByMlLib <br/>
  * Description: <br/>
  * date: 2019/4/23 18:18<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object JavaALSExampleByMlLib {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JavaALSExample").setMaster("local[4]")
    val jsc = new JavaSparkContext(conf)
    val data = jsc.textFile("data/sample_movielens_ratings.txt")
    val ratings = data.map(new Function[String, Rating]() {
      override def call(s: String): Rating = {
        val sarray = StringUtils.split(StringUtils.trim(s), "::")
        new Rating(sarray(0).toInt, sarray(1).toInt, sarray(2).toDouble)
      }
    })
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 6
    val model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01)
    // Evaluate the model on rating data
    val userProducts = ratings.map(new Function[Rating, Tuple2[AnyRef, AnyRef]]() {
      override def call(r: Rating) = new Tuple2[AnyRef, AnyRef](r.user, r.product)
    })
    // 预测的评分
    val predictions = JavaPairRDD.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD.map(new Function[Rating, Tuple2[Tuple2[Integer, Integer], Double]]() {
      override def call(r: Rating) = new Tuple2[Tuple2[Integer, Integer], Double](new Tuple2[Integer, Integer](r.user, r.product), r.rating)
    }))
    val ratesAndPreds = JavaPairRDD.fromJavaRDD(ratings.map(new Function[Rating, Tuple2[Tuple2[Integer, Integer], Double]]() {
      override def call(r: Rating) = new Tuple2[Tuple2[Integer, Integer], Double](new Tuple2[Integer, Integer](r.user, r.product), r.rating)
    })).join(predictions).asInstanceOf[JavaPairRDD[Tuple2[Integer, Integer], Tuple2[Double, Double]]]
    // 得到按照用户ID排序后的评分列表 key:用户id
    val fromJavaRDD = JavaPairRDD.fromJavaRDD(ratesAndPreds.map(new Function[Tuple2[Tuple2[Integer, Integer], Tuple2[Double, Double]], Tuple2[Integer, Tuple2[Integer, Double]]]() {
      @throws[Exception]
      override def call(t: Tuple2[Tuple2[Integer, Integer], Tuple2[Double, Double]]) = new Tuple2[Integer, Tuple2[Integer, Double]](t._1._1, new Tuple2[Integer, Double](t._1._2, t._2._2))
    })).sortByKey(false)
    //		List<Tuple2<Integer,Tuple2<Integer,Double>>> list = fromJavaRDD.collect();
    //		for(Tuple2<Integer,Tuple2<Integer,Double>> t:list){
    //			System.out.println(t._1+":"+t._2._1+"===="+t._2._2);
    //		}
    val ratesAndPredsValues = ratesAndPreds.values
    val MSE = JavaDoubleRDD.fromRDD(ratesAndPredsValues.map(new Function[Tuple2[Double, Double], AnyRef]() {
      override def call(pair: Tuple2[Double, Double]): Any = {
        val err = pair._1 - pair._2
        err * err
      }
    }).rdd).mean
    try
      FileUtils.deleteDirectory(new File("result"))
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    ratesAndPreds.repartition(1).saveAsTextFile("result/ratesAndPreds")
    //为指定用户推荐10个产品
    val recommendProducts = model.recommendProducts(2, 10)
    log.info("get recommend result:{}", util.Arrays.toString(recommendProducts))
    // 为所有用户推荐TOP N个物品
    //model.recommendUsersForProducts(10);
    // 为所有物品推荐TOP N个用户
    //model.recommendProductsForUsers(10)
    model.userFeatures.saveAsTextFile("result/userFea")
    model.productFeatures.saveAsTextFile("result/productFea")
    log.info("Mean Squared Error = {}", MSE)
  }

}
