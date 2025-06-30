import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SparkSession, DataFrame, Encoders}
import org.apache.spark.sql.functions._
import play.api.libs.json.Json
import com.typesafe.config.ConfigFactory

case class Location(lat: Double, lon: Double)

case class IoTEvent(
  device_id: String,
  timestamp: String,
  location: Location,
  temperature: Double,
  humidity: Double,
  tank_level: Int
)

object MinioWriter {

  implicit val locationFormat = Json.format[Location]
  implicit val iotEventFormat = Json.format[IoTEvent]

  def writeKafkaEventsToMinio(spark: SparkSession): Unit = {
    // val config = ConfigFactory.load()
    // val minioBucket = config.getString("minio.bucket")
    // val minioPath = s"s3a://$minioBucket/iot-events/"

    // val kafkaDF = spark.readStream
    //   .format("kafka")
    //   .option("kafka.bootstrap.servers", config.getString("kafka.bootstrap.servers"))
    //   .option("subscribe", config.getString("kafka.topic"))
    //   .option("startingOffsets", "earliest")
    //   .load()

    // val jsonSchema = Encoders.product[IoTEvent].schema

    // val parsedEvents = kafkaDF
    //   .selectExpr("CAST(value AS STRING) as json")
    //   .select(from_json(col("json"), jsonSchema).as("data"))
    //   .select("data.*")

    // parsedEvents.writeStream
    //   .format("json")
    //   .option("checkpointLocation", "/tmp/spark-checkpoints/iot-events") // Local temp
    //   .option("path", minioPath)
    //   .outputMode("append")
    //   .start()
    //   .awaitTermination()
    import spark.implicits._

    println("Initializing Kafka stream...")

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "iot-events")
      .option("startingOffsets", "earliest")
      .load()

    println("Kafka stream successfully initialized.")

    val parsedDf = kafkaDf.selectExpr("CAST(value AS STRING)").as[String]

    println("Starting transformation of incoming data...")

    val transformedDf = parsedDf.map { json =>
      // Replace with your own parsing logic
      println(s"Parsing JSON: $json")
      json // Assuming raw string is stored as-is for now
    }

    println("Transformation complete. Writing to MinIO...")

    val query = transformedDf.writeStream
      .format("json")
      .option("path", "s3a://iot-events/iot-events/")
      .option("checkpointLocation", "/tmp/spark-checkpoint/")
      .outputMode("append")
      .start()
    
    //val query = parsedDf.writeStream
    //  .format("console")
    //  .option("checkpointLocation", "/tmp/spark-checkpoint/iot-events")
    //  .outputMode("append")
    //  .start()


    println("WriteStream started successfully. Waiting for termination...")

    query.awaitTermination()

    println("Streaming query terminated.")
  }
}

object Main extends App {
  val config = ConfigFactory.load()

  val minioEndpoint = config.getString("minio.endpoint")
  val minioAccessKey = config.getString("minio.access-key")
  val minioSecretKey = config.getString("minio.secret-key")

  val spark = SparkSession.builder()
    .appName("IoTEventWriter")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", minioEndpoint)
    .config("spark.hadoop.fs.s3a.access.key", minioAccessKey)
    .config("spark.hadoop.fs.s3a.secret.key", minioSecretKey)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()

  println("Starting IoTEvent consumer and writer to MinIO...")

  // Appel de la fonction définie dans ton objet MinioWriter
  MinioWriter.writeKafkaEventsToMinio(spark)

  // Spark Structured Streaming ne se termine jamais sans interruption manuelle
  // donc pas besoin de spark.stop()
}
