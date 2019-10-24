package main.shuashuaspark_ALS

import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ClassName: shuashuademo <br/>
  * Description: <br/>
  * date: 2019/4/17 17:24<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object shuashuademo {
  val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)

  val ss = new SparkSession()
  //创建一个密集型局部向量（两种）
  var dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
  val densityVector = Vectors.dense(1.0, 0.0, 3.0)
  //创建一个稀疏局部向量（两种）1.并行数组2.seq
  val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
  val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

  val pos = LabeledPoint(1.0, dv)
  val value: RDD[LabeledPoint] = MLUtils.loadLabeledPoints(sc, "path")
  val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

}
