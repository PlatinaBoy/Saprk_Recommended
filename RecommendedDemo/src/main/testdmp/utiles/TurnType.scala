package main.testdmp.utiles

/**
  * ClassName: TurnType <br/>
  * Description: <br/>
  * date: 2019/4/15 17:38<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object TurnType {
  def toDouble(str:String)={
    try{
      str.toDouble
    }catch {
      case _:Exception=>0.0
    }
  }


  def  toInt(str:String): Unit ={
    try{
      str.toDouble
    }catch {
      case _:Exception=>0
    }
  }

}
