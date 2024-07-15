import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

object ConsumerApp extends App {
  // Create SparkSession
  val spark: SparkSession = SparkSession
    .builder()
    .master("spark://192.168.1.50:7077")
    .appName("KafkaConsumer")
    .getOrCreate()

  // Read from Kafka topic
  val kafkaDF: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.1.50:9092")
    .option("subscribe", "books")
    .option("startingOffsets", "earliest")
    .load()

  // Extract the JSON string from the value column and parse it
  val parsedDF: DataFrame = kafkaDF.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"),
      new StructType()
        .add("timestamp", StringType)
        .add("Name", StringType)
        .add("Author", StringType)
        .add("User Rating", DoubleType)
        .add("Reviews", LongType)
        .add("Price", DoubleType)
        .add("Year", IntegerType)
        .add("Genre", StringType)
    ).as("data"))
    .select("data.*")

  // Process the parsed data (for example, print it to console)
  val query = parsedDF.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()

  // Await stream termination
  query.awaitTermination()

  // Stop SparkSession
  spark.stop()
}
