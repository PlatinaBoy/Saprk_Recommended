package recommend.portraitBase

import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import utils.movieYearRegex

import scala.collection.JavaConversions._
import scala.util.control.Breaks

/**
  * Desc: 基于用户画像的计算，对用户进行电影推荐
  */
object portraitBaseRecommandApp {
  def main(args : Array[String]): Unit = {

    //设置hive访问host以及端口  hadoop9
    //注意：实际使用的时候，替换成自己服务器集群中，hive的host以及访问端口
val HIVE_METASTORE_URIS = "thrift://hive-host-01:9083,thrift://hive-host-02:9083"
    System.setProperty("hive.metastore.uris", HIVE_METASTORE_URIS)

    //构建一个通用的sparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("portrait-base-re")
      .enableHiveSupport()
      .getOrCreate()

    //获取rating评分数据集
    val ratingData = sparkSession.sql("select userid,movieid,rate  from mite8.mite_ratings")

    //获取用户的年份数据，并在hive中进行抽取，并对相同用户以及年份的进行合并，权重去sum(rate)
    val userYear = sparkSession.sql("select userid,year,sum(rate) as rate from (select userid,rate,regexp_extract(title,'.*\\\\(([1-9][0-9][0-9][0-9])\\\\).*',1) as year from mite8.mite_ratings aa join mite8.mite_movies bb on aa.movieid = bb.movieid) aaa group by aaa.userid,aaa.year order by userid,rate desc")

    //获取用户的类别偏好数据
    val userGenre = sparkSession.sql("select aa.userid as userid,genre,sum(rate) as rate from mite8.mite_ratings aa join mite8.mite_movies bb on aa.movieid = bb.movieid group by aa.userid,genre order by userid,rate desc")

    //获取电影数据的基本属性
    val moviesData = sparkSession.sql("select aa.movieid as movieid,title,genre,avg(bb.rate) as rate from mite8.mite_movies aa join mite8.mite_ratings bb on aa.movieid = bb.movieid group by aa.movieid,aa.title,aa.genre")

    //获取电影tags数据
    val tagsData = sparkSession.sql("select movieid,tag from mite8.mite_tags").rdd

    System.out.println("=================001 GET DATA===========================")

    ///==============Get Movie Content================////

    //进行tags标签的处理,包括分词，去除停用词等等
    val tagsStandardize = tagsData.map{
      f =>
        val movieid = f.get(0)
        //进行逻辑判断，size>3的进行标准化处理
        val tag = if (f.get(1).toString.split(" ").size <= 3) {
          f.get(1)
        }else{
          //进行主题词抽取(能屏蔽掉停用词)
          HanLP.extractKeyword(f.get(1).toString, 20).toSet.mkString(" ")
        }
        (movieid,tag)
    }

    System.out.println("=================002.1 HANLP===========================")

    //进行相似tag合并操作，最终返回依然是(mvieid,tag)集合，但tag会做预处理
    val tagsStandardizeTmp = tagsStandardize.collect()
    val tagsSimi = tagsStandardize.map{
      f=>
        var retTag = f._2
        if (f._2.toString.split(" ").size == 1) {
          var simiTmp = ""

          val tagsTmpStand = tagsStandardizeTmp
                        .filter(_._2.toString.split(" ").size != 1 )
                        .filter(f._2.toString.size < _._2.toString.size)
                        .sortBy(_._2.toString.size)

          var x = 0
          val loop = new Breaks

          tagsTmpStand.map{
            tagTmp=>
              val flag = getEditSize(f._2.toString,tagTmp._2.toString)
              if (flag == 1){
                retTag = tagTmp._2
                loop.break()
              }
          }

          (f._1,retTag)
        } else {
          f
        }
    }

    System.out.println("=================002.2 TAG SIMI===========================")

    //先将预处理之后的movie-tag数据进行，统计频度，作为tag权重,形成(movie,tagList(tag,score))这种数据集
    val movieTagList = tagsSimi.map(f=>((f._1,f._2),1)).reduceByKey(_+_).groupBy(k=>k._1._1).map{
      f=>
        (f._1,f._2.map{
          ff=>
            (ff._1._2,ff._2)
        }.toList.sortBy(_._2).reverse.take(10).toMap)
    }

    System.out.println("=================002.3 MOVIE-TAG OK===========================")

    //进行电影genre以及year属性处理
    val moviesGenresYear = moviesData.rdd.map{
      f=>
        val movieid = f.get(0)
        val genres = f.get(2)
        val year = movieYearRegex.movieYearReg(f.get(1).toString)
        val rate = f.get(3).asInstanceOf[java.math.BigDecimal].doubleValue()
        (movieid,(genres,year,rate))
    }

    System.out.println("=================002.4 MOVIE-GENRE/YEAR OK===========================")

    //聚合movie数据,并做过滤排序，排除质量较差的movie候选集，以rate
    val movieContent = movieTagList.join(moviesGenresYear).filter(f=>f._2._2._3 < 2.5).sortBy(f=>f._2._2._3,false).map{
      f=>
        //userid，taglist，genre，year，rate
        (f._1,f._2._1,f._2._2._1,f._2._2._2,f._2._2._3)
    }.collect()

    System.out.println("=================003 MOVIE-CONTENT OK===========================")

    ///==============Get User Content================////

    //先将预处理之后的movie-tag数据进行dataframe
    val schemaString = "movieid tag"
    val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))
    val tagsSimiDataFrame = sparkSession.createDataFrame(tagsSimi.map(f=>Row(f._1,f._2.toString.trim)),schema)

    //对rating(userid,movieid,rate)，tags(movieid,tag)进行join，以movieid关联
    //join步骤：将(userId, movieId, rate)与(movieId, tag)按照movieId字段进行连接
    val tagRateDataFrame = ratingData.join(tagsSimiDataFrame,ratingData("movieid")===tagsSimiDataFrame("movieid"),"inner").select("userid","tag","rate")

    System.out.println("=================004.1 USER-TAG JOIN===========================")

    //reduce步骤：将(userId, tag, rate)中(userId, tag)相同的分数rate相加
    val userPortraitTag = tagRateDataFrame.groupBy("userid","tag").sum("rate").rdd.map{
      f=>
        (f.get(0),f.get(1),f.get(2).asInstanceOf[java.math.BigDecimal].doubleValue())
    }.groupBy(f=>f._1).map{
      f=>
        val userid = f._1
        val tagList = f._2.toList.sortBy(_._3)
          .reverse.map(k=>(k._2,k._3)).take(20)
        (userid,tagList.toMap)
    }

    System.out.println("=================004.2 USER-TAG OK===========================")

    //进行用户year的处理=>(userid,yearList(10))
    val userPortraitYear = userYear.rdd.map(f=>(f.get(0),f.get(1),f.get(2))).groupBy(f=>f._1).map{
      f=>
        val userid = f._1
        val yearList = f._2.map(f=>(f._2,f._3.asInstanceOf[java.math.BigDecimal].doubleValue())).toList.take(10)
        (userid,yearList)
    }

    System.out.println("=================004.3 USER-YEAR OK===========================")

    //进行用户genre处理
    val userPortraitGenre = userGenre.rdd.map(f=>(f.get(0),f.get(1),f.get(2))).groupBy(f=>f._1).map{
      f=>
        val userid = f._1
        val genreList = f._2.map(f=>(f._2,f._3.asInstanceOf[java.math.BigDecimal].doubleValue())).toList.take(10)
        (userid,genreList)
    }

    System.out.println("=================004.4 USER-GENRE OK===========================")

    //获取用户已经评分的电影列表,需要在计算的时候移除已经看过的电影
    val userMovieGet = ratingData.rdd.map(f=>(f.get(0),f.get(1))).groupByKey()

    //进行用户画像属性关联，并计算最佳推荐列表
    val portraitBaseReData = userPortraitTag.join(userPortraitYear).join(userPortraitGenre).join(userMovieGet).map{
      f=>
        val userid = f._1
        val userTag = f._2._1._1._1
        val userYear = f._2._1._1._2
        val userGenre = f._2._1._2
        //用于做差集计算，移除已经看过的电影
        val userMovieList = f._2._2.toList
        val movieRe = movieContent.map{
          ff=>
            val movieid = ff._1
            val movieTag = ff._2
            val movieGenre = ff._3
            val movieYear = ff._4
            val movieRate = ff._5
            val simiScore = getSimiScore(userTag ,movieTag,userGenre,movieGenre,userYear,movieYear,movieRate)
            (movieid,simiScore)
        }.diff(userMovieList).sortBy(k=>k._2).reverse.take(20)
        (userid,movieRe)
    }.flatMap(f=>f._2.map(ff=>(f._1,ff._1,ff._2)))

    System.out.println("=================005 USER-MOVIE OK===========================")

    //保存推荐列表
    val schemaPortraitStr = "userid movieid score"
    val schemaPortrait = StructType(schemaPortraitStr.split(" ").map(fieldName=>StructField(fieldName,if (fieldName.equals("score")) DoubleType else  StringType,true)))
    val portraitBaseReDataFrame = sparkSession.createDataFrame(portraitBaseReData.map(f=>Row(f._1,f._2,f._3)),schemaPortrait)
    //将结果存入hive
    val portraitBaseReTmpTableName = "mite_portraitbasetmp"
    val portraitBaseReTableName = "mite8.mite_portrait_base_re"
    portraitBaseReDataFrame.registerTempTable(portraitBaseReTmpTableName)
    sparkSession.sql("insert into table " + portraitBaseReTableName + " select * from " + portraitBaseReTmpTableName)

    System.out.println("=================006 SAVE OK===========================")

  }

  //计算用户与电影之间的相似度
  def getSimiScore(userTag:Map[Any,Double],movieTag:Map[Any,Int],
                   userGenre:List[(Any,Double)],movieGenre:Any,
                   userYear:List[(Any,Double)],movieYear:Any,
                   movieRate:Double): Double ={
    val tagSimi = getCosTags(userTag,movieTag)
    val genreSimi = getGenreOrYear(userGenre,movieGenre)
    val yearSimi = getGenreOrYear(userYear,movieYear)
    val rateSimi = getRateSimi(movieRate)
    val score = 0.4*genreSimi + 0.3*tagSimi + 0.1*yearSimi + 0.2*rateSimi
    score
  }

  //计算年份的相似度
  def getRateSimi(rate2: Double): Double ={
    if (rate2 >= 5) {
      1
    } else {
      rate2 / 5
    }
  }

  //计算List对应的单个属性的相似度
  def getGenreOrYear(userGenreOrYear:List[(Any,Double)],movieGenreOrYear:Any):Double = {
    if (userGenreOrYear.contains(movieGenreOrYear)) {
      val maxGenreOrYear = userGenreOrYear.sortBy(f=>f._2).reverse.get(1)._2
      val currentScore = userGenreOrYear.toMap.get(movieGenreOrYear).get
      if (currentScore > maxGenreOrYear) 1 else currentScore/maxGenreOrYear
    } else {
      0
    }
  }

  //计算两个MapTag的余弦值
  def getCosTags(listTagsCurrent:Map[Any,Double],listTagsTmp:Map[Any,Int]): Double = {

    //分子累和部分
    var xySum: Double = 0
    //分母开方前的Ai平方累和部分
    var aSquareSum: Double = 0
    //分母开方前的Bi平方累和部分
    var bSquareSum: Double = 0

    val tagsA = listTagsCurrent.map(f => f._1).toList
    val tagsB = listTagsTmp.map(f => f._1).toList
    tagsA.union(tagsB).map {
      f =>
        if (listTagsCurrent.contains(f)) (aSquareSum += listTagsCurrent.get(f).get * listTagsCurrent.get(f).get)
        if (listTagsTmp.contains(f)) (bSquareSum += listTagsTmp.get(f).get * listTagsTmp.get(f).get)
        if (listTagsCurrent.contains(f) && listTagsTmp.contains(f)) (xySum += listTagsCurrent.get(f).get * listTagsTmp.get(f).get)
    }

    if (aSquareSum != 0 && bSquareSum != 0) {
      xySum / (Math.sqrt(aSquareSum) * Math.sqrt(bSquareSum))
    } else {
      0
    }
  }


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
