package ClickhouseDatasource

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

class CHInputPartition(val schema: StructType, val query: String, host: String) extends InputPartition {
  override def preferredLocations(): Array[String] = Array(host)
}