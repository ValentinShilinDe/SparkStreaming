package SparkStructuredStreamingKafka

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.SparkSession

object Producer {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaProducer")
      .getOrCreate()

    import spark.implicits._

    val schema = new StructType()
      .add("Name", StringType)
      .add("Author", StringType)
      .add("Rating", DoubleType)
      .add("Reviews", LongType)
      .add("Price", DoubleType)
      .add("Year", IntegerType)
      .add("Genre", StringType)

    val df = spark
      .readStream
      .schema(schema)
      .option("header", "true")
      .csv(args.apply(0))

    val stream = df
      .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", args.apply(1))
      .option("topic", args.apply(2))
      .option("checkpointLocation", s"${args.apply(0)}tmp/")
      .start()

    stream.awaitTermination()

    spark.stop()
  }
}