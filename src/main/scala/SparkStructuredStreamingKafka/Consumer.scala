package SparkStructuredStreamingKafka

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Consumer {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaConsumer")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args.apply(0))
      .option("subscribe", args.apply(1))
      .option("startingOffsets", "earliest")
      .load()

    val parsedDF: DataFrame = kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"),
        new StructType()
          .add("Name", StringType)
          .add("Author", StringType)
          .add("Rating", DoubleType)
          .add("Reviews", LongType)
          .add("Price", DoubleType)
          .add("Year", IntegerType)
          .add("Genre", StringType)
      ).as("data"))
      .select("data.*")
      .filter(col("Rating") < 4)

    val query = parsedDF.writeStream
      .format("parquet")
      .outputMode("append")
      .option("checkpointLocation", s"${args.apply(2)}tmp/")
      .option("path", args.apply(2))
      .start()

    // Await stream termination
    query.awaitTermination()

    // Stop SparkSession
    spark.stop()
  }
}
