package testdmp.confighelp

import com.typesafe.config.{Config, ConfigFactory}

//专门加载配置文件类
object ConfigHelper {
  //指点加载的路径并且加载
  private lazy val load: Config = ConfigFactory.load()
  //获取原始日志文件路径
  val logPath: String = load.getString("LogPath")
  //加载parquet文件路径
  val parquetPath: String = load.getString("ParquetPath")
  //加载parquet文件序列化
  val serializer: String = load.getString("spark.serializer")
  //加载Javajdbc
  val driver: String = load.getString("db.default.driver")
  val url: String = load.getString("db.default.url")
  val user: String = load.getString("db.default.user")
  val password: String = load.getString("db.default.password")

}
