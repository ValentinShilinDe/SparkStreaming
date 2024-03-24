package ClickhouseDatasource

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

import java.util

class CHScanBuilder(schema: StructType, properties: util.Map[String, String]) extends ScanBuilder {
  override def build(): Scan = new CHScan(schema, properties)
}