import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import play.api.libs.json._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.influxdb.client.{InfluxDBClient, InfluxDBClientFactory}
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import sttp.client3._
import sttp.client3.playJson._
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
// logger


case class IoTEvent(
  device_id: String,
  timestamp: String,
  location: Location,
  temperature: Double,
  humidity: Double,
  tank_level: Int
)

case class Location(lat: Double, lon: Double)

case class DeviceHealthMetrics(
  device_id: String,
  avg_temperature: Double,
  avg_humidity: Double,
  avg_tank_level: Double,
  temperature_variance: Double,
  humidity_variance: Double,
  health_score: Double,
  alert_level: String
)

case class GeographicMetrics(
  region: String,
  lat_range: String,
  lon_range: String,
  device_count: Long,
  avg_temperature: Double,
  avg_humidity: Double,
  min_temperature: Double,
  max_temperature: Double
)

case class TankAnalytics(
  device_id: String,
  current_level: Double,
  consumption_rate: Double,
  days_remaining: Double,
  refill_priority: String
)

case class TemporalTrends(
  hour_of_day: Int,
  avg_temperature: Double,
  avg_humidity: Double,
  avg_tank_level: Double,
  event_count: Long
)

object ServiceAnalytics extends LazyLogging {
  
  implicit val locationFormat: Format[Location] = Json.format[Location]
  implicit val iotEventFormat: Format[IoTEvent] = Json.format[IoTEvent]

  private val config = ConfigFactory.load()
  private val minioEndpoint = config.getString("minio.endpoint")
  private val minioAccessKey = config.getString("minio.access-key")
  private val minioSecretKey = config.getString("minio.secret-key")
  private val minioBucket = config.getString("minio.bucket")
  private val influxUrl = config.getString("influxdb.url")
  private val influxToken = config.getString("influxdb.token")
  private val influxOrg = config.getString("influxdb.org")
  private val influxBucket = config.getString("influxdb.bucket")

  def createSparkSession(): SparkSession = {
    println("MINIO ENDPOINT: " + minioEndpoint)
    println("MINIO ACCESS KEY: " + minioAccessKey)
    println("MINIO SECRET KEY: " + minioSecretKey)
    SparkSession.builder()
      .appName("IoT Analytics Service")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", minioEndpoint)
      .config("spark.hadoop.fs.s3a.access.key", minioAccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", minioSecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()
  }

  def loadDataFromMinio(spark: SparkSession): DataFrame = {
    logger.info(s"Loading data from Minio bucket: $minioBucket")
    
    // val schema = StructType(Array(
    //   StructField("device_id", StringType, true),
    //   StructField("timestamp", StringType, true),
    //   StructField("location", StructType(Array(
    //     StructField("lat", DoubleType, true),
    //     StructField("lon", DoubleType, true)
    //   )), true),
    //   StructField("temperature", DoubleType, true),
    //   StructField("humidity", DoubleType, true),
    //   StructField("tank_level", IntegerType, true)
    // ))

    spark.read.option("multiline", "false").json(s"s3a://$minioBucket/iot-events/*.json")
  }
}


object Main extends App {
    import java.io._
import scala.io.Source

def mergeCSVFiles(inputDir: String, outputFile: String): Unit = {
    val dir = new File(inputDir)
    if (!dir.exists || !dir.isDirectory) {
        println(s"Input directory $inputDir does not exist or is not a directory")
        return
    }

    // List all CSV files in the directory
    val csvFiles = dir.listFiles.filter(f => f.isFile && f.getName.endsWith(".csv")).toList.sorted

    if (csvFiles.isEmpty) {
        println(s"No CSV files found in $inputDir")
        return
    }

    val writer = new PrintWriter(new File(outputFile))

    try {
        // Read header from the first file
        val firstFile = csvFiles.head
        val header = Source.fromFile(firstFile).getLines().take(1).toList.head
        writer.println(header)

        // Append all lines except header from each CSV
        csvFiles.foreach { file =>
        val lines = Source.fromFile(file).getLines().drop(1) // drop header
        lines.foreach(writer.println)
        }
        println(s"Successfully merged ${csvFiles.size} CSV files into $outputFile")
    } finally {
        writer.close()
    }
    }

    val spark = ServiceAnalytics.createSparkSession()
    import spark.implicits._

    println("Spark session created successfully!")
    // load a json file from Minio
    val doudou = spark.read
      .option("multiline", "false")
      .json("s3a://iot-events/iot-events/*.json")
    
    println(s"Loaded ${doudou.count()} events from Minio")
    // show the first 5 rows in a file
    val df = doudou
    .withColumn("lat", col("location.lat"))
    .withColumn("lon", col("location.lon"))
    .drop("location")

    // flattenedDF
    //     .coalesce(1) // reduce to a single partition (single output file)
    //     .write
    //     .mode("overwrite")
    //     .option("header", "true")
    //     .csv("output_single_file")

    // Create output directory if it doesn't exist
    val outputDir = "./output"
    Files.createDirectories(Paths.get(outputDir))
    val alertThreshold = 60.0

    // 1. Fermenters by number of alerts
    val fermentersByAlerts = df.filter(col("humidity") > alertThreshold)
    .groupBy("device_id")
    .count()
    .orderBy(desc("count"))

    fermentersByAlerts.write.mode("overwrite").option("header", true).csv(s"$outputDir/fermenters_by_alerts")

    // 2. Alert distribution by location
    val alertsByLocation = df.filter(col("humidity") > alertThreshold)
    .groupBy("lat", "lon")
    .count()
    .orderBy(desc("count"))

    alertsByLocation.write.mode("overwrite").option("header", true).csv(s"$outputDir/alerts_by_location")

    // 3. Fermenters with least humidity variation (stddev)
    val variations = df.groupBy("device_id")
    .agg(stddev("humidity").alias("stddev_humidity"))
    .orderBy(asc("stddev_humidity"))

    variations.write.mode("overwrite").option("header", true).csv(s"$outputDir/least_variations")

    // 4. Mean of humidity, temperature, etc.
    val means = df.agg(
    mean("humidity").alias("avg_humidity"),
    mean("temperature").alias("avg_temperature")
    )

    means.write.mode("overwrite").option("header", true).csv(s"$outputDir/means")

    println("✅ Analytics job results written to ./output/")


    // stop the Spark session
    spark.stop()
    println("Spark session stopped gracefully.")
    // Merge all CSV files in the output directory into a single file
    mergeCSVFiles("output/least_variations", "output/least_variations/least_variations.csv")
    mergeCSVFiles("output/fermenters_by_alerts", "output/fermenters_by_alerts/fermenters_by_alerts.csv")
    mergeCSVFiles("output/alerts_by_location", "output/alerts_by_location/alerts_by_location.csv")
    mergeCSVFiles("output/means", "output/means/means.csv")
}
