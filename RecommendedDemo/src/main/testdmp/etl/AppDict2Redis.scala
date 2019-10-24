package main.testdmp.etl



import main.testdmp.utiles.GetJedis
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ClassName: AppDict2Redis <br/>
  * Description: 将app字典表写入redis
  * date: 2019/4/15 14:28<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object AppDict2Redis {

  def main(args: Array[String]): Unit = {
    //sparkconf
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(s"${this.getClass.getName}")
    //sparkcontext
    val sc = new SparkContext(conf)


    //读取数据
    val dictLines = sc.textFile("")
    //数据清洗（清洗规则）
    val dictRDD = dictLines.map(_.split("",-1)).filter(_.length>=5).map(arr=>(arr(1),arr(2)))
    dictRDD.foreachPartition(partition=>{
      //获取连接
      val jedis = GetJedis.getJedis(1)
      partition.foreach(tu=>{
        //插入数据，表名为appdict,key为appid,value为appname

        jedis.hset("appdict",tu._1,tu._2)
      })
      //关闭线程
      jedis.close()
    })
    //释放资源
    sc.stop()
  }

}
