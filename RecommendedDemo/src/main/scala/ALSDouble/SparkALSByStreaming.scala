package ALSDouble

import java.util

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{Function, VoidFunction}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

/**
  * @author platina
  * @category 基于Spark-streaming、kafka的实时推荐模板DEMO 原系统中包含商城项目、logback、flume、hadoop
  *           The real time recommendation template DEMO based on Spark-streaming and Kafka contains the mall project, logback, flume and Hadoop in the original system
  */
object SparkALSByStreaming {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val KAFKA_ADDR = ""
  private val TOPIC = ""
  private val HDFS_ADDR = ""
  private val MODEL_PATH = ""

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root") // 设置权限用户
    val sparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val jssc = new JavaStreamingContext(sparkConf, Durations.seconds(6))
    // key是topic名称,value是线程数量
    val kafkaParams = new util.HashMap[String, String]
    // 指定broker在哪
    kafkaParams.put("metadata.broker.list", KAFKA_ADDR)
    val topicsSet = new util.HashSet[String]
    topicsSet.add(TOPIC) // 指定操作的topic
    // Create direct kafka stream with brokers and topics
    // createDirectStream()

    val messages = KafkaUtils.createDirectStream(jssc, classOf[String], classOf[String], classOf[StringDecoder], classOf[StringDecoder], kafkaParams, topicsSet)
    val lines = messages.map(new Function[Tuple2[String, String], String]() {
      override def call(tuple2: Tuple2[String, String]): String = tuple2._2
    })
    val ratingsStream = lines.map(new Function[String, Rating]() {
      override def call(s: String): Rating = {
        val sarray = StringUtils.split(StringUtils.trim(s), ",")
        new Rating(sarray(0).toInt, sarray(1).toInt, sarray(2).toDouble)
      }
    })
    // 进行流推荐计算
    ratingsStream.foreachRDD(new VoidFunction[JavaRDD[Rating]]() {
      @throws[Exception]
      override def call(ratings: JavaRDD[Rating]): Unit = { //  获取到原始的数据集
        val sc = ratings.context
        val textFileRDD = sc.textFile(HDFS_ADDR + "/flume/logs", 3)
        // 读取原始数据集文件
        val originalTextFile = textFileRDD.toJavaRDD
        val originaldatas = originalTextFile.map(new Function[String, Rating]() {
          override def call(s: String): Rating = {
            val sarray = StringUtils.split(StringUtils.trim(s), ",")
            new Rating(sarray(0).toInt, sarray(1).toInt, sarray(2).toDouble)
          }
        })
        log.info("========================================")
        log.info("Original TextFile Count:{}", originalTextFile.count) // HDFS中已经存储的原始用户行为日志数据
        log.info("========================================")
        //  将原始数据集和新的用户行为数据进行合并
        val calculations = originaldatas.union(ratings)
        log.info("Calc Count:{}", calculations.count)
        // Build the recommendation model using ALS
        val rank = 10
        // 模型中隐语义因子的个数
        val numIterations = 6 // 训练次数
        // 得到训练模型
        if (!ratings.isEmpty) { // 如果有用户行为数据
          val model = ALS.train(JavaRDD.toRDD(calculations), rank, numIterations, 0.01)
          //  判断文件是否存在,如果存在 删除文件目录
          val hadoopConfiguration = sc.hadoopConfiguration
          hadoopConfiguration.set("fs.defaultFS", HDFS_ADDR)
          val fs = FileSystem.get(hadoopConfiguration)
          val outpath = new Path(MODEL_PATH)
          if (fs.exists(outpath)) {
            log.info("########### 删除" + outpath.getName + " ###########")
            fs.delete(outpath, true)
          }
          // 保存model
          model.save(sc, HDFS_ADDR + MODEL_PATH)
          //  读取model
          val modelLoad = MatrixFactorizationModel.load(sc, HDFS_ADDR + MODEL_PATH)
          // 为指定用户推荐10个商品
          var userId = 0
          while ( {
            userId < 30
          }) { // streaming_sample_movielens_ratings.txt
            val recommendProducts = modelLoad.recommendProducts(userId, 10)
            log.info("get recommend result:{}", recommendProducts) {
              userId += 1;
              userId - 1
            }
          }
        }
      }
    })
    // ==========================================================================================
    jssc.start()
    try jssc.awaitTermination()
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    // Local Model
    try Thread.sleep(10000000)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    // jssc.stop();
    // jssc.close();
  }
}
