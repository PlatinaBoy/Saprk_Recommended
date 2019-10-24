package main.testdmp.utiles

import org.apache.spark.sql.Row

/**
  * ClassName: AdKPIs <br/>
  * Description: <br/>
  * date: 2019/4/15 15:56<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object AdKPIs {
  def AdKPIs(row:Row): Unit ={
    /**
      * 判断点击量指标
      * ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDERID
      */
    val REQUESTMODE = row.getAs[Int]("requestmode")
    val PROCESSNODE = row.getAs[Int]("processnode")
    val adList: List[Double] = if (REQUESTMODE == 1 && PROCESSNODE == 1) {
      List[Double](1, 0, 0)
    } else if (REQUESTMODE == 1 && PROCESSNODE == 2) {
      List[Double](1, 1, 0)
    } else if (REQUESTMODE == 1 && PROCESSNODE == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
    //获取并判断 ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDERID
    val ISEFFECTIVE = row.getAs[Int]("iseffective")
    val ISBILLING = row.getAs[Int]("isbilling")
    val ISBID = row.getAs[Int]("isbid")
    val ISWIN = row.getAs[Int]("iswin")
    val ADORDERID = row.getAs[Int]("adorderid")
    //获取WinPrice，adpayment
    val WinPrice = row.getAs[Double]("winprice")
    val adpayment = row.getAs[Double]("adpayment")

    val costList: List[Double] = if (ISEFFECTIVE == 1 && ISBILLING == 1 && ISBID == 1 && ADORDERID != 0) {
      List[Double](1, 0, 0, 0)
    } else if (ISEFFECTIVE == 1 && ISBILLING == 1 && ISWIN == 1) {
      List[Double](0, 1, adpayment / 1000, WinPrice / 1000)
    } else {
      List[Double](0, 0, 0, 0)
    }
    costList
    //展示量点击量
    val showList = if (REQUESTMODE == 2 && ISEFFECTIVE == 1) {
      List[Double](1, 0)
    } else if (REQUESTMODE == 3 && ISEFFECTIVE == 1) {
      List[Double](0, 1)
    } else {
      List[Double](0, 0)
    }
    showList
    adList++costList++showList



  }


}
