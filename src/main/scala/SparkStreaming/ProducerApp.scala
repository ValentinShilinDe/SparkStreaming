import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object ProducerApp extends App {
  // Create SparkSession
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("KafkaProducer")
    .getOrCreate()

  // Sample data array
  val dataArray = Seq(
    ("book1", "author1", 4.5, 100L, 25.0, 2021, "Fiction"),
    ("book2", "author2", 4.2, 150L, 30.0, 2020, "Fantasy"),
    ("book3", "author3", 4.7, 200L, 20.0, 2019, "Mystery")
  )

  // Convert array to DataFrame
  import spark.implicits._
  val staticDF: DataFrame = dataArray.zipWithIndex.toDF("data", "staticIndex")
    .select(
      col("data._1").as("Name"),
      col("data._2").as("Author"),
      col("data._3").as("Rating"),
      col("data._4").as("Reviews"),
      col("data._5").as("Price"),
      col("data._6").as("Year"),
      col("data._7").as("Genre"),
      col("staticIndex")
    )

  // Create a streaming DataFrame using the rate source to simulate a stream
  val rateDF = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .load()
    .withColumn("streamingIndex", col("value"))

  // Join the static DataFrame with the streaming DataFrame
  val joinedDF = rateDF
    .join(staticDF, rateDF("value") % staticDF.count() === staticDF("staticIndex"))
    .select("timestamp", "Name", "Author", "Rating", "Reviews", "Price", "Year", "Genre")

  // Convert the joined DataFrame to JSON and write to Kafka
  val stream = joinedDF
    .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9093")
    .option("topic", "test_topic1")
    .option("checkpointLocation", "checkdir")
    .start()

  // Await stream termination
  stream.awaitTermination()

  // Stop SparkSession
  spark.stop()
}
