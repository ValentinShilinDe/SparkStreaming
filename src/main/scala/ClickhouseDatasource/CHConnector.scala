package ClickhouseDatasource

import org.apache.spark.sql.types._

object CHConnector {
  val singledataSource = new BalancedClickhouseDataSource("jdbc://clickhous://127.0.0.1:9000")
  val connection: ClickHouseConnection = singledataSource.getConnection


  def convertToSparkType(chType:String): DataType = {
    chType match {
      case "String" => StringType
      case "UInt64" => IntegerType
      case _ => StringType
    }
  }

  def getSchema(tableName: String): StructType = {
    var res: StructType = new StructType()
    val rs = connection.createStatement().executeQuery(s"DESCRIBE TABLE $tableName")

    while(rs.next) {
      res = res.add(StructField(rs.getString(rs.getString("name")),
        convertToSparkType(rs.getString("type"))))
    }
    res
  }

  def getHostName: String =
    connection.createStatement().executeQuery(s"""SELECT getHostName()""").getString(1)

  def getPartitions(tableName: String): List[String] = {
    var res: List[String] = List()
    val rs = connection.createStatement().executeQuery(
      s"""SELECT partition, FROM system.parts where table = '$tableName'"""
    )

    while (rs.next) {
      res = res :+ rs.getString("partition")
    }
    res
  }


}