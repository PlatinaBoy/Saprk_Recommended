package ALS

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession


/**
  * 使用org.apache.spark.ml.recommendation.ALS来计算，并且使用了spark2.0的新特性SparkSession来实现推荐
  */
object ALSDemo2 {

  //定义个类，来保存一次评分
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  //把一行转换成一个评分类
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]) = {
    //SparkSession是spark2.0的全新切入点，以前都是sparkcontext创建RDD的，StreamingContext，sqlContext，HiveContext。
    //DataDrame提供的API慢慢的成为新的标准API，我们需要1个新的切入点来构建他，这个就是SparkSession
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "D:/ideaWorkspace/ScalaSparkMl/spark-warehouse")
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()
    import spark.implicits._
    /*
    read方法返回的是一个DataFrameReader类，可以转换为DataFrame
    DataFrameReader类的textFile方法：加载文本数据，返回为Dataset
    使用一个函数parseRating处理一行数据
     */

    val ratings = spark.read.textFile("")
      .map(parseRating)
      .toDF()
    //对数据进行随机分割，80%为训练样例，剩下的为测试样例。

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    /*
     Build the recommendation model using ALS on the training data
    使用训练数据训练模型
    这里的ALS是import org.apache.spark.ml.recommendation.ALS，不是mllib中的哈
    setMaxiter设置最大迭代次数
    setRegParam设置正则化参数，日lambda这个不是更明显么
    setUserCol设置用户id列名
    setItemCol设置物品列名
    setRatingCol设置打分列名
     */
    val als = new ALS()

    als.setRank(10)
      .setMaxIter(5) //迭代次数，最大值好像不能>=30，否则会Java Stack Overflow 错误。
      .setRegParam(0.1) //正则化参数，使每次迭代平滑一些
      .setUserCol("userId") //设置用户的id
      .setItemCol("movieId") //设置商品id
      .setRatingCol("rating") //设置评分

    //fit给输出的数据，训练模型，fit返回的是ALSModel类,调用算法开始训练（用定义好的算法对训练数据进行训练），得到一个模型
    val model = als.fit(training)

    //使用测试数据计算模型的误差平方和
    //transform方法把数据dataset换成dataframe类型，预测数据
    val predictions = model.transform(test)

    /*
     RegressionEvaluator这个类是用户评估预测效果的，预测值与原始值
     这个setLabelCol要和als设置的setRatingCol一致，不然会报错哈
     RegressionEvaluator的setPredictionCol必须是prediction因为，ALSModel的默认predictionCol也是prediction
     如果要修改的话必须把ALSModel和RegressionEvaluator一起修改
     model.setPredictionCol("prediction")和evaluator.setPredictionCol("prediction")
     setMetricName这个方法，评估方法的名字，一共有哪些呢？
     rmse-平均误差平方和开根号
     mse-平均误差平方和
     mae-平均距离（绝对）
     r2-没用过不知道
     这里建议就是用rmse就好了，其他的基本都没用，当然还是要看应用场景，这里是预测分值就是用rmse。
     如果是预测距离什么的mae就不从，看场景.
     */
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    //计算出错误率
    val rmse = evaluator.evaluate(predictions)
    println("Calculate error rate" = rmse)


    //给每一个用户推荐他可能最喜欢的10个商品

    val userRecs = model.recommendForAllUsers(10)

    //每个商品最喜欢的十个人
    val shopRecs = model.recommendForAllItems(10)
    // 数据落地
    userRecs.repartition(2).write.json("")
    shopRecs.repartition(2).write.json("")
    spark.stop()
  }
}
