package ClickhouseDatasource

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import java.util

class CHBatch(schema: StructType, properties: util.Map[String, String]) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    val parts = CHConnector.getPartitions(properties.get("tableName"))
    val hostName = CHConnector.getHostName
    val query = s"Select * from ${properties.get("tableName")} where ${properties.get("partitionKey")}"
    parts.map(p=> new CHInputPartition(schema, s"$query $p", hostName)).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new CHPartitionReaderFactory(schema)

}