package recommend.CFBase.Item

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Desc: 基于Item的协同过滤，对用户进行推荐
  */
object ItemBaseRecommandApp {
  def main(args : Array[String]): Unit = {

    //设置hive访问host以及端口  hadoop9
    //注意：实际使用的时候，替换成自己服务器集群中，hive的host以及访问端口
val HIVE_METASTORE_URIS = "thrift://hive-host-01:9083,thrift://hive-host-02:9083"
    System.setProperty("hive.metastore.uris", HIVE_METASTORE_URIS)

    //构建一个通用的sparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("item-base-re")
      .enableHiveSupport()
      .getOrCreate()

    //获取rating评分数据集并转换为RDD，鉴于机器配置，降低数据量
    val ratingData = sparkSession.sql("select userid,movieid,rate  from mite8.mite_ratings limit 50000")
      .rdd.map(f=>(java.lang.Long.parseLong(f.get(0).toString),java.lang.Long.parseLong(f.get(1).toString),f.get(2).asInstanceOf[java.math.BigDecimal].doubleValue()))

    System.out.println("=================001 GET DATA OK===========================")

    //过滤多于的(user,item,rate),卡 (best-loved) 100上限，降低计算量
    val ratings = ratingData.groupBy(k=>k._1).flatMap(f=>f._2.toList.sortWith((f,y)=>f._3>y._3).take(100))

    // 获取每个的评分数量,把key为item对应的id,把格式转换为 (item,(user,item,rate))
    val item2manyUser = ratings.groupBy(tup => tup._2)
    val numRatersPerItem = item2manyUser.map(grouped => (grouped._1, grouped._2.size))

    // 通过item id进行关联,转换为 (user,item,rate,numRaters)
    val ratingsWithSize = item2manyUser.join(numRatersPerItem).
      flatMap(joined => {
        joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))
      })

    // 为形成自身join矩阵，通过keyby进行获取
    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)

    //通过上一步获取的自身矩阵，计算半矩阵，减少计算量
    val ratingPairs =ratings2.join(ratings2).filter(f => f._2._1._2 < f._2._2._2)

    System.out.println("=================002 METRICS OK===========================")

    // 针对每个item id计算其相似矩阵，并拆分单独的模块进行计算
    val tempVectorCalcs =
      ratingPairs.map(data => {
        val key = (data._2._1._2, data._2._2._2)
        val stats =
          (data._2._1._3 * data._2._2._3, // rate 1 * rate 2
            data._2._1._3,                // rate item 1
            data._2._2._3,                // rate item 2
            math.pow(data._2._1._3, 2),   // square of rate item 1
            math.pow(data._2._2._3, 2),   // square of rate item 2
            data._2._1._4,                // number of raters item 1
            data._2._2._4)                // number of raters item 2
        (key, stats)
      })
    val vectorCalcs = tempVectorCalcs.groupByKey().map(data => {
      val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map(f => f._1).sum
      val ratingSum = vals.map(f => f._2).sum
      val rating2Sum = vals.map(f => f._3).sum
      val ratingSq = vals.map(f => f._4).sum
      val rating2Sq = vals.map(f => f._5).sum
      val numRaters = vals.map(f => f._6).max
      val numRaters2 = vals.map(f => f._7).max
      (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
    })
    val inverseVectorCalcs = vectorCalcs.map(x=>((x._1._2,x._1._1),(x._2._1,x._2._2,x._2._4,x._2._3,x._2._6,x._2._5,x._2._8,x._2._7)))
    val vectorCalcsTotal = vectorCalcs ++ inverseVectorCalcs

    // 转换公式: cosSim *size/(numRaters*math.log10(numRaters2+10))
    val tempSimilarities =
      vectorCalcsTotal.map(fields => {
        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))*size/(numRaters*math.log10(numRaters2+10))
        (key._1,(key._2, cosSim))
      })

    val similarities = tempSimilarities.groupByKey().flatMap(x=>{
      x._2.map(temp=>(x._1,(temp._1,temp._2))).toList.sortWith((a,b)=>a._2._2>b._2._2).take(50)
    })

    /////////////////////////////////////////////////////////////

    // 转换数据为 (item,(user,rate))
    val ratingsInverse = ratings.map(rating=>(rating._2,(rating._1,rating._3)))

    //进一步转换数据 ((user,item),(sim,sim*rating))
    // ratingsInverse.join(similarities) 变为格式 (Item,((user,rate),(item,similar)))的数据形态
    val statistics = ratingsInverse.join(similarities)
      .map(x=>((x._2._1._1,x._2._2._1),(x._2._2._2,x._2._1._2*x._2._2._2)))

    // 推测的结果数据((user,item),predict)
    val predictResult = statistics.reduceByKey((x,y)=>((x._1+y._1),(x._2+y._2)))
      .map(x=>(x._1,x._2._2/x._2._1))

    //进行结果过滤，看过的不需要再进行推荐
    val filterItem = ratingData.map(x=>((x._1,x._2),Double.NaN))
    val totalScore = predictResult ++ filterItem

    //最终结果进行裁剪
    val itemBaseDataFrame = totalScore.reduceByKey(_+_).filter(x=> !(x._2 equals(Double.NaN))).
      map(x=>(x._1._1,x._1._2,x._2)).groupBy(x=>x._1).map{
        f=>
          val userid = f._1
          val movieReList = f._2.toList.sortBy(k=>k._3).reverse.take(20)
          (userid,movieReList)
      }.flatMap(y=>y._2.map(v=>(y._1,v._2,v._3))).map(f=>Row(f._1,f._2,f._3))

    System.out.println("=================003 ITEM SIMI OK===========================")

    //DataFrame格式化申明
    val schemaString = "userid movieid score"
    val schemaItemBase = StructType(schemaString.split(" ")
      .map(fieldName=>StructField(fieldName,if (fieldName.equals("score")) DoubleType else  LongType,true)))
    val movieItemBaseDataFrame = sparkSession.createDataFrame(itemBaseDataFrame,schemaItemBase)

    //将结果存入hive
    val itemBaseReTmpTableName = "mite_itembasetmp"
    val itemBaseReTableName = "mite8.mite_item_base_re"

    movieItemBaseDataFrame.registerTempTable(itemBaseReTmpTableName)
    sparkSession.sql("insert into table " + itemBaseReTableName + " select * from " + itemBaseReTmpTableName)

    System.out.println("=================006 SAVE OK===========================")

  }

  //相关性计算函数
  def correlation(size : Double, dotProduct : Double, ratingSum : Double,
                  rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

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
