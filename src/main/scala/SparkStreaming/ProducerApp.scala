
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.SparkFiles


object ProducerApp extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("spark://192.168.1.50:7077")
    .appName("KafkaProducer")
    .getOrCreate()

  val csvDF = spark.read
  .option("header", "true")
  .option("sep", ",") 
  .option("inferSchema", "true") 
  .csv("/home/bdron/source/bestsellers with categories.csv")

  // преобразование DataFrame в RDD для добавления индекса записи
  val rddWithIndex = csvDF.rdd.zipWithIndex.map {
    case (row, index) => Row.fromSeq(row.toSeq :+ index.toInt + 1)
  }

  // определение новой схемы с дополнительным столбцом индекса
  val newSchema = StructType(csvDF.schema.fields :+ StructField("index", IntegerType, false))

  // преобразование RDD обратно в DataFrame с новой схемой
  val indexedDF = spark.createDataFrame(rddWithIndex, newSchema)

  // создание стримингового DataFrame с использованием RateSource
  val rateDF = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1) 
    .load()
    
  // присоединение DataFrame с временной меткой к RateSource
  val streamingDF = rateDF
    .join(indexedDF, rateDF("value") === indexedDF("index"))
    .selectExpr("CAST(value AS STRING) AS key", "to_json(struct(Name, Author, `User Rating`, Reviews, Price, Year, Genre, timestamp)) AS value")

    // Запись DataFrame в Kafka
  streamingDF.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092") 
    .option("topic", "books") 
    .option("checkpointLocation", "/tmp/checkpoint") 
    .start()
    .awaitTermination()


  spark.stop()
}