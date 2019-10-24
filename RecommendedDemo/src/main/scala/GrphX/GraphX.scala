package GrphX


import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 2019年4月10日 12:26:00
  * GraohX demos
  *
  */
object GraphX {
  def main(args: Array[String]): Unit = {
    //sparkconf
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    //sparkcontext
    val sc = new SparkContext(conf)
    //read data
    val sourceLine = sc.textFile("")
    //创建点集合
    val pointRdd: RDD[(Long, String)] = sourceLine.map(_.split("", -1)).flatMap(arr => {
      arr.map(name => (name.hashCode.toLong, name))
    }).distinct()
    pointRdd
    //创建边集合
    val edgeRdd: RDD[Edge[Int]] = sourceLine.map(_.split((""), -1)).flatMap(arr => {
      arr.map(name => Edge(arr.head.hashCode, name.hashCode, 0))
    })
    edgeRdd
    //创建图对象
    val graph: Graph[String, Int] = Graph(pointRdd, edgeRdd)

    val ver = graph.connectedComponents().vertices
    //ver.map(tu=>(tu._2,Set(tu._1))).reduceByKey(_++_).foreach(println)
    //好友聚集
    ver.join(pointRdd).map {
      case (maxId, (id, name)) => (id, name)
    }.reduceByKey(_.concat("").concat(_)).foreach(println)
    //释放资源

    sc.stop()
  }

}
