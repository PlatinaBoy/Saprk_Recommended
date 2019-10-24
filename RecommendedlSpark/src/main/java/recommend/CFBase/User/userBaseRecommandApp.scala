package recommend.CFBase.User

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Desc: 基于User的协同过滤，对用户进行推荐
  */
object userBaseRecommandApp {
  def main(args : Array[String]): Unit = {

    //设置hive访问host以及端口  hadoop9
    //注意：实际使用的时候，替换成自己服务器集群中，hive的host以及访问端口
val HIVE_METASTORE_URIS = "thrift://hive-host-01:9083,thrift://hive-host-02:9083"
    System.setProperty("hive.metastore.uris", HIVE_METASTORE_URIS)

    //构建一个通用的sparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("user-base-re")
      .enableHiveSupport()
      .getOrCreate()

    //获取rating评分数据集并转换为RDD,鉴于机器配置，降低数据量
    val ratingData = sparkSession.sql("select userid,movieid,rate  from mite8.mite_ratings limit 50000")
      .rdd.map(f=>(java.lang.Long.parseLong(f.get(0).toString),java.lang.Long.parseLong(f.get(1).toString),f.get(2).asInstanceOf[java.math.BigDecimal].doubleValue()))

    System.out.println("=================001 GET DATA OK===========================")

    //过滤多于的(user,item,rate),卡 (best-loved) 100上限，降低计算量
    val ratings = ratingData.groupBy(k=>k._1).flatMap(f=>f._2.toList.sortWith((f,y)=>f._3>y._3).take(100))

    //一个用户对应多个item
    val user2manyItem = ratings.groupBy(tup=>tup._1)

    //获取一个用户对应的item数量
    val numPrefPerUser = user2manyItem.map(grouped=>(grouped._1,grouped._2.size))

    //通过join操作，获取 (user, item, rate, numPrefs).
    val ratingsWithSize = user2manyItem.join(numPrefPerUser).
      flatMap(joined=>{
        joined._2._1.map(f=>(f._1,f._2,f._3,joined._2._2))
      })

    //数据转换(user, item, rating, numPrefs) ==>(item,(user, item, rating, numPrefs))
    val ratings2 = ratingsWithSize.keyBy(tup=>tup._2)
    //进一步转换为 (t,iterator((u1,t,pref1,numpref1),(u2,t,pref2,numpref2))) and u1<u2
    val ratingPairs = ratings2.join(ratings2).filter(f=>f._2._1._1<f._2._2._1)

    //计算的预处理
    val tempVectorCalcs = ratingPairs.map(data=>{
      val key = (data._2._1._1,data._2._2._1)
      val stats =
        (data._2._1._3*data._2._2._3,//rate 1 * rate 2
          data._2._1._3, //rate user 1
          data._2._2._3, //rate user 2
          math.pow(data._2._1._3, 2), //square of rate user 1
          math.pow(data._2._2._3,2), //square of rate user 2
          data._2._1._4,  //num prefs of user 1
          data._2._2._4) //num prefs of user 2
      (key,stats)
    })
    val vectorCalcs = tempVectorCalcs.groupByKey().map(data=>{
      val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map(f=>f._1).sum
      val ratingSum = vals.map(f=>f._2).sum
      val rating2Sum = vals.map(f=>f._3).sum
      val ratingSeq = vals.map(f=>f._4).sum
      val rating2Seq = vals.map(f=>f._5).sum
      val numPref = vals.map(f=>f._6).max
      val numPref2 = vals.map(f=>f._7).max
      (key,(size,dotProduct,ratingSum,rating2Sum,ratingSeq,rating2Seq,numPref,numPref2))
    })

    //对半矩阵进行对称化处理，转换为完整矩阵
    val inverseVectorCalcs = vectorCalcs.map(x=>((x._1._2,x._1._1),(x._2._1,x._2._2,x._2._4,x._2._3,x._2._6,x._2._5,x._2._8,x._2._7)))
    val vectorCalcsTotal = vectorCalcs ++ inverseVectorCalcs

    System.out.println("=================002 METRICS OK===========================")

    // 进行相似计算
    val tempSimilarities =
      vectorCalcsTotal.map(fields => {
        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))*
          size/(numRaters*math.log10(numRaters2+10))
        (key._1,(key._2, cosSim))
      })

    val similarities = tempSimilarities.groupByKey().flatMap(x=>{
      x._2.map(temp=>(x._1,(temp._1,temp._2))).toList.sortWith((a,b)=>a._2._2>b._2._2).take(50)
    })

    // 原始数据变换为 (user,(item,raing))
    val ratingsInverse = ratings.map(rating=>(rating._1,(rating._2,rating._3)))

    //进一步转为((user,item),(sim,sim*rating))
    // ratingsInverse.join(similarities) 变为格式 (user,((item,rating),(user2,similar)))的数据形态
    val statistics = ratingsInverse.join(similarities)
      .map(x=>((x._2._2._1,x._2._1._1),(x._2._2._2,x._2._1._2*x._2._2._2)))

    // 获取推荐列表结果 ((user,item),predict)
    val predictResult = statistics.reduceByKey((x,y)=>((x._1+y._1),(x._2+y._2)))
      .map(x=>(x._1,x._2._2/x._2._1))

    //过滤已经看过的
    val filterItem = ratings.map(x=>((x._1,x._2),Double.NaN))
    val totalScore = predictResult ++ filterItem

    //最终结果进行裁剪
    val userBaseDataFrame = totalScore.reduceByKey(_+_).filter(x=> !(x._2 equals(Double.NaN))).
      map(x=>(x._1._1,x._1._2,x._2)).groupBy(x=>x._1).map{
      f=>
        val userid = f._1
        val movieReList = f._2.toList.sortBy(k=>k._3).reverse.take(20)
        (userid,movieReList)
    }.flatMap(y=>y._2.map(v=>(y._1,v._2,v._3))).map(f=>Row(f._1,f._2,f._3))


    System.out.println("=================003 USER SIMI OK===========================")

    //DataFrame格式化申明
    val schemaString = "userid movieid score"
    val schemaUserBase = StructType(schemaString.split(" ")
      .map(fieldName=>StructField(fieldName,if (fieldName.equals("score")) DoubleType else  LongType,true)))
    val movieUserBaseDataFrame = sparkSession.createDataFrame(userBaseDataFrame,schemaUserBase)

    //将结果存入hive
    val itemBaseReTmpTableName = "mite_userbasetmp"
    val itemBaseReTableName = "mite8.mite_user_base_re"

    movieUserBaseDataFrame.registerTempTable(itemBaseReTmpTableName)
    sparkSession.sql("insert into table " + itemBaseReTableName + " select * from " + itemBaseReTmpTableName)

    System.out.println("=================006 SAVE OK===========================")

  }

  //相关性计算函数
  def correlation(size : Double, dotProduct : Double, ratingSum : Double,
                  rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)+1

    numerator / denominator
  }

  //矩阵转换函数
  //RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
  //where w = # actualPairs / (# actualPairs + # virtualPairs).
  def regularizedCorrelation(size : Double, dotProduct : Double, ratingSum : Double,
                             rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double,
                             virtualCount : Double, priorCorrelation : Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  //求余弦夹角函数
  def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }
}
