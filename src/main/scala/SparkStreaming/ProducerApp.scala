package SparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object ProducerApp extends App{

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("WriterJob")
    .getOrCreate()

  val userSchema = new StructType()
    .add("Name", "string")
    .add("Author", "string")
    .add("Rating", "double")
    .add("Reviews", "long")
    .add("Price", "double")
    .add("Year", "int")
    .add("Genre", "string")

  import spark.implicits._
  spark.readStream
    .format("csv")
    .schema(userSchema)
    .load("src/main/resources/bestsellers.csv")
    .withColumnRenamed("User Rating", "Rating")
    .as[Book]
    .toJSON
    .selectExpr("CAST(value as STRING)")
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29093")
    .option("topic", "books")
    .start()


}
