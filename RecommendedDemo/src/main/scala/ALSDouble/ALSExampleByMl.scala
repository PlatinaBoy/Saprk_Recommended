package ALSDouble

import java.io.Serializable

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.slf4j.LoggerFactory

/**
  * @author platina
  * @category ALS-WR
  */
object ALSExampleByMl {

  //logger
private val log = LoggerFactory.getLogger("ALSExampleByMl")

  object Rating {
    def parseRating(str: String): ALSExampleByMl.Rating = {
      val fields = str.split("::")
      if (fields.length != 4) throw new IllegalArgumentException("")
      val userId = fields(0).toInt
      val movieId = fields(1).toInt
      //      val rating = Float.parseFloat(fields(2))
      //      val timestamp = Long.parseLong(fields(3))
      val rating = fields(2).toFloat
      val timestamp = fields(3).toLong
      new ALSExampleByMl.Rating(userId, movieId, rating, timestamp)
    }
  }

  class Rating() extends Serializable {
    // 0::2::3::1424380312
    private var userId = 0 // 0
    private var movieId = 0 // 2
    private var rating = 0.0F // 3
    private var timestamp = 0L // 1424380312
    def this(userId: Int, movieId: Int, rating: Float, timestamp: Long) {
      this()
      this.userId = userId
      this.movieId = movieId
      this.rating = rating
      this.timestamp = timestamp
    }

    def getUserId: Int = userId

    def getMovieId: Int = movieId

    def getRating: Float = rating

    def getTimestamp: Long = timestamp
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val jsc = new JavaSparkContext(conf)
    val sqlContext = new SQLContext(jsc)
    val ratingsRDD = jsc.textFile("")
      .map(new Function[String, ALSExampleByMl.Rating]() {
      override def call(str: String): ALSExampleByMl.Rating = Rating.parseRating(str)
    })
    val ratings = sqlContext.createDataFrame(ratingsRDD, classOf[ALSExampleByMl.Rating])
    val splits = ratings.randomSplit(Array[Double](0.8, 0.2))
    // //对数据进行分割，80%为训练样例，剩下的为测试样例。
    val training = splits(0)
    val test = splits(1)
    // Build the recommendation model using ALS on the training data
    //    //    val als = new ALS().setMaxIter(5).setRegParam // 设置迭代次数
    //    val als = new ALS().setMaxIter(5).setRegParam(9)
    //    0.01.setUserCol // //正则化参数，使每次迭代平滑一些，此数据集取0.1好像错误率低一些。
    //    "userId".setItemCol("movieId").setRatingCol("rating")
    val als = new ALS()

    als.setRank(10)
      .setMaxIter(5) //迭代次数，最大值好像不能>=30，否则会Overflow 错误。
      .setRegParam(0.1) //正则化参数，使每次迭代平滑一些
      .setUserCol("userId") //设置用户的id
      .setItemCol("movieId") //设置商品id
      .setRatingCol("rating") //设置评分


    val model = als.fit(training)
    // //调用算法开始训练
    val itemFactors = model.itemFactors
    itemFactors.show(1500)
    val userFactors = model.userFactors
    userFactors.show()
    // Evaluate the model by computing the RMSE on the test data
    val rawPredictions = model.transform(test)
    //对测试数据进行预测
    val predictions = rawPredictions.withColumn("rating",
      rawPredictions.col("rating").cast(DataTypes.DoubleType))
      .withColumn("prediction", rawPredictions.col("prediction").cast(DataTypes.DoubleType))
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    log.info("Root-mean-square error = {} ", rmse)
    jsc.stop()
  }
}
