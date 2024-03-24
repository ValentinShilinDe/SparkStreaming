package ClickhouseDatasource

import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class CHTable(
             schema: StructType,
             partitioning: Array[Transform],
             properties: util.Map[String, String]
             ) extends SupportsRead {
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new CHScanBuilder(schema, options)

  def name(): String = properties.get("tableName")
  def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    util.Set.of(TableCapability.BATCH_READ)

}