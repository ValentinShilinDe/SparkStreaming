import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object ProducerApp extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("KafkaProducer")
    .getOrCreate()

  val inputData = spark.read
    .format("csv")
    .option("header", "true")
    .load("bestsellers with categories.csv")

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

  val stream = inputData
    .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29093")
    .option("topic", "books")
    .option("checkpointLocation", "checkdir")
    .start()

  stream.awaitTermination()

  spark.stop()
}
