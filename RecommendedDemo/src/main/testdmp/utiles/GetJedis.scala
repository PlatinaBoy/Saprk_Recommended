package main.testdmp.utiles

import redis.clients.jedis.{Jedis, JedisPool}

/**
  * ClassName: GetJedis <br/>
  * Description:Redis <br/>
  * date: 2019/4/15 15:50<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object GetJedis {
  def getJedis(index:Int=0) ={
    //创建连接池
    val pool = new JedisPool()
    //拿出连接
    val jedis: Jedis = pool.getResource
    jedis.select(index)
    jedis
  }

}
