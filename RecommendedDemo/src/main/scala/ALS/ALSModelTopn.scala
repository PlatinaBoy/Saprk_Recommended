import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}


object ALSModelTopn {

  def main(args: Array[String]): Unit = {

    //给用户推荐
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val shop = sc.textFile("")
    val ratings = shop.map(line => {
      val fields = line.split("")
      val rating = Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      val timestamp = fields(3).toLong % 5
      (rating)

    })

    val model = MatrixFactorizationModel.load(sc, "")

    //选择一个用户
    val user = 5
    val myRating = ratings.filter(x => x.user == 5)

    //该用户已经消费了的物品
    val myRateItem = myRating.map(x => x.product).collect().toSet

    //给用户5推荐前评分前10的物品
    val recommendations = model.recommendProducts(user, 10)
    recommendations.map(x => {
      println(x.user + "-->" + x.product + "-->" + x.rating)
    })
  }
}



