package SparkStreaming

case class Book (
                  name: Option[String],
                  author: Option[String],
                  rating: Option[Double],
                  reviews: Option[Long],
                  price: Option[Double],
                  year: Option[Int],
                  genre: Option[String]
                )

