package ClickhouseDatasource

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

import java.util

class CHScan(schema: StructType, properties: util.Map[String, String]) extends Scan {

  override def readSchema(): StructType = schema

  override def description(): String = "ch_scan"

  override def toBatch: Batch = new CHBatch(schema, properties)

}