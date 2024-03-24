package ClickhouseDatasource

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class CHTableProvider extends TableProvider with DataSourceRegister {
  def inferSchema(options: CaseInsensitiveStringMap): StructType = CHConnector.getSchema(options.get("tableName"))

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new CHTable(schema, partitioning, properties)

  def shortName(): String  = "clickhous"

  override def supportsExternalMetadata(): Boolean = true
}