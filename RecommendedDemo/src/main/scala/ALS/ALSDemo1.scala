package ALS


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable

/**
  *
  */
object ALSDemo1 {

  //参数含义
  //input表示数据路径
  //kryo表示是否使用kryo序列化
  //numIterations迭代次数
  //lambda正则化参数
  //numUserBlocks用户的分块数
  //numProductBlocks物品的分块数
  //implicitPrefs这个参数没用过，但是通过后面的可以推断出来了，是否开启隐藏的分值参数阈值，
  // 预测在那个级别才建议推荐，这里是5分制度的，详细看后面代码
  case class Params(
                     input: String = null,
                     output: String = null,
                     kryo: Boolean = false,
                     numIterations: Int = 20,
                     lambda: Double = 1.0,
                     rank: Int = 10,
                     numUserBlocks: Int = -1,
                     numProductBlocks: Int = -1,
                     implicitPrefs: Boolean = false)

  def main(args: Array[String]) {
    val defaultParams = Params()

    //规定参数的输入方式 --rank 10 这种
    //我个人习惯为直接用空格分割（如果参数不对，给予提示），当然下面这种更规范化和人性化，还有默认参数的
    //以后再研究OptionParser用法，不过他这种参数用法挺好用的
    val parser = new OptionParser[Params]("Mllib 的ALS") {
      head("MovieLensALS: an example app for ALS on MovieLens data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text("use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks} (auto)")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numProductBlocks} (auto)")
        .action((x, c) => c.copy(numProductBlocks = x))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))

      arg[String]("<output>")
        .required()
        .text("output Model Path")
        .action((x, c) => c.copy(output = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.MovieLensALS \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --rank 5 --numIterations 20 --lambda 1.0 --kryo \
          |  data/mllib/sample_movielens_data.txt
        """.stripMargin)
    }

    //map但是只运行1次
    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"MovieLensALS with $params").setMaster("local").set("spark.sql.warehouse.dir", "E:/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    //如果参数设置了kryo序列化没那么需要注册序列化的类和配置序列化的缓存，模板照着写就是了
    //使用序列化是为传输的时候速度更快，我没有使用这个，因为反序列话也需要一定的时间，我是局域网搭建spark集群的（机子之间很快）。
    // 如果是在云搭建集群可以考虑使用
    if (params.kryo) {
      conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating]))
        .set("spark.kryoserializer.buffer", "8m")
    }
    val sc = new SparkContext(conf)

    //设置log基本，生产也建议使用WARN
    Logger.getRootLogger.setLevel(Level.WARN)

    //得到因此的级别
    val implicitPrefs = params.implicitPrefs

    //读取数据，并通过是否设置了分值阈值来修正评分
    //官方推荐是，只有哦大于3级别的时候才值得推荐
    //且下面的代码，implicitPrefs，直接就是默认5 Must see，按道理会根据自己对分数阈值的预估，rating减去相应的值，比如fields(2).toDouble - 2.5
    //5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5
    //现在是5分值的映射关系，如果是其他分值的映射关系有该怎么做？还不确定，个人建议别使用这个了。
    //经过下面代码推断出，如果implicitPrefs=true或者flase，true的意思是，预测的分数要大于2.5（自己设置），才能推荐给用户，小了，没有意义
    //它引入implicitPrefs的整体含义为，只有用户对物品的满意达到一定的值，才推荐，不然推荐不喜欢的没有意思，所以在构建样本的时候，会减去相应的值fields(2).toDouble - 2.5（自己设置）
    //这种理论是可以的，但是还有一个理论，不给用户推荐比给用户推荐错了还要严重（有人提出过），不推荐产生的效果还要严重，还有反向推荐，
    //我把implicitPrefs叫做分值阈值
    val ratings = sc.textFile(params.input).map { line =>
      val fields = line.split("::")
      if (implicitPrefs) {
        /*
         * MovieLens ratings are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the predicted rating is less than 3.
         * To map ratings to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
      } else {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    }.cache()

    //计算一共有多少样本数
    val numRatings = ratings.count()
    //计算一共有多少用户
    val numUsers = ratings.map(_.user).distinct().count()
    //计算应该有多少物品
    val numMovies = ratings.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    //按80%训练，20%验证分割样本
    val splits = ratings.randomSplit(Array(0.8, 0.2))

    //把训练样本缓存起来，加快运算速度
    val training = splits(0).cache()

    //构建测试样，我先翻译下他说的英文哈。
    //分值为0表示，我对物品的评分不知道，一个积极有意义的评分表示：有信心预测值为1
    //一个消极的评分表示：有信心预测值为0
    //在这个案列中，我们使用的加权的RMSE，这个权重为自信的绝对值（命中就为1，否则为0）
    //关于误差，在预测和1,0之间是不一样的，取决于r 是正，还是负
    //这里splits已经减了分值阈值了，所以>0 =1 else 0的含义是，1表示分值是大于分值阈值的，这里是大于2.5,0表示小于2.5
    val test = if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */
      splits(1).map(x => Rating(x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
    } else {
      splits(1)
    }.cache()

    //训练样本量和测试样本量
    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    //这里应为不适用ratings了，释放掉它占的内存
    ratings.unpersist(blocking = false)

    //setRank设置随机因子，就是隐藏的属性
    //setIterations设置最大迭代次数
    //setLambda设置正则化参数
    //setImplicitPrefs 是否开启分值阈值
    //setUserBlocks设置用户的块数量，并行化计算,当特别大的时候需要设置
    //setProductBlocks设置物品的块数量
    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(training)

    //训练的样本和测试的样本的分值全部是减了2.5分的
    //测试样本的分值如果大于0为1，else 0，表示分值大于2.5才预测为Ok

    //计算rmse
    val rmse = computeRmse(model, test, params.implicitPrefs)

    println(s"Test RMSE = $rmse.")

    //保存模型，模型保存路劲为
    model.save(sc, params.output)
    println("模型保存成功，保存路劲为：" + params.output)

    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean)
  : Double = {

    //内部方法含义如下
    // 如果已经开启了implicitPref那么，预测的分值大于0的为1，小于0的为0，没有开启的话，就是用原始分值
    //min(r,1.0)求预测分值和1.0那个小，求小值，然后max(x,0.0)求大值， 意思就是把预测分值大于0的为1，小于0 的为0
    //这样构建之后预测的预测值和测试样本的样本分值才一直，才能进行加权rmse计算
    def mapPredictedRating(r: Double): Double = {
      if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    }

    //根据模型预测，用户对物品的分值，predict的参数为RDD[(Int, Int)]
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))

    //mapPredictedRating把预测的分值映射为1或者0
    //join连接原始的分数,连接的key为x.user, x.product
    //values方法表示只保留预测值，真实值
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values

    //最后计算预测与真实值的平均误差平方和
    //这是先每个的平方求出来，然后再求平均值，最后开方
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
}

