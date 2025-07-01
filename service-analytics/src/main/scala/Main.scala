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

  // 1. Device Health Analysis
    def analyzeDeviceHealth(df: DataFrame): DataFrame = {
    logger.info("Analyzing device health metrics...")
        df.groupBy("device_id")
        .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("tank_level").alias("avg_tank_level"),
        variance("temperature").alias("temperature_variance"),
        variance("humidity").alias("humidity_variance"),
        count("*").alias("reading_count")
        )
        .withColumn("health_score", 
        when(col("temperature_variance") > 10 || col("humidity_variance") > 100, 0.3)
        .when(col("avg_temperature") < 20 || col("avg_temperature") > 35, 0.5)
        .when(col("avg_humidity") < 40 || col("avg_humidity") > 80, 0.6)
        .when(col("avg_tank_level") < 20, 0.4)
        .otherwise(0.9)
        )
        .withColumn("alert_level",
        when(col("health_score") < 0.5, "CRITICAL")
        .when(col("health_score") < 0.7, "WARNING")
        .otherwise("HEALTHY")
        )
    }  
    // 2. Geographic Distribution Analysis
  def analyzeGeographicDistribution(df: DataFrame): DataFrame = {
    logger.info("Analyzing geographic distribution...")
  
    df.withColumn("lat_bucket", round(col("lat"), 1))
      .withColumn("lon_bucket", round(col("lon"), 1))
      .withColumn("region", concat(col("lat_bucket"), lit(","), col("lon_bucket")))
      .groupBy("region", "lat_bucket", "lon_bucket")
      .agg(
        countDistinct("device_id").alias("device_count"),
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature")
      )
      .withColumn("lat_range", concat(col("lat_bucket"), lit("±0.05")))
      .withColumn("lon_range", concat(col("lon_bucket"), lit("±0.05")))
  }

  // 3. Tank Level Analytics
   def analyzeTankLevels(df: DataFrame): DataFrame = {
     logger.info("Analyzing tank levels and consumption patterns...")
  
     val windowSpec = Window.partitionBy("device_id").orderBy("timestamp")
  
     df.withColumn("timestamp_unix", unix_timestamp(col("timestamp")))
       .withColumn("prev_level", lag("tank_level", 1).over(windowSpec))
       .withColumn("prev_timestamp", lag("timestamp_unix", 1).over(windowSpec))
       .withColumn("level_change", col("tank_level") - col("prev_level"))
       .withColumn("time_diff_hours", (col("timestamp_unix") - col("prev_timestamp")) / 3600)
       .filter(col("level_change") < 0) // Only consumption (negative changes)
       .groupBy("device_id")
       .agg(
         last("tank_level").alias("current_level"),
         avg(col("level_change") / col("time_diff_hours")).alias("consumption_rate_per_hour")
       )
       .withColumn("days_remaining", 
         when(col("consumption_rate_per_hour") < 0, 
           col("current_level") / abs(col("consumption_rate_per_hour")) / 24)
         .otherwise(999)
       )
       .withColumn("refill_priority",
         when(col("days_remaining") < 1, "URGENT")
         .when(col("days_remaining") < 3, "HIGH")
         .when(col("days_remaining") < 7, "MEDIUM")
         .otherwise("LOW")
       )
   }

  // 4. Temporal Trends Analysis
    def analyzeTemporalTrends(df: DataFrame): DataFrame = {
        logger.info("Analyzing temporal trends...")
        
        df.withColumn("hour_of_day", hour(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
        .groupBy("hour_of_day")
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("tank_level").alias("avg_tank_level"),
            count("*").alias("event_count")
        )
        .orderBy("hour_of_day")
    }

    def writeToInfluxDB(dataFrame: DataFrame, measurement: String): Unit = {
     logger.info(s"Writing $measurement data to InfluxDB...")
  
     val client = InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray, influxOrg, influxBucket)
     val writeApi = client.getWriteApiBlocking()
  
     try {
       dataFrame.collect().foreach { row =>
         val point = Point.measurement(measurement)
           .time(System.currentTimeMillis(), WritePrecision.MS)
      
         // Add all fields from the row
         row.schema.fields.foreach { field =>
           val value = row.getAs[Any](field.name)
           if (value != null) {
             field.dataType match {
               case StringType => point.addTag(field.name, value.toString)
               case DoubleType => point.addField(field.name, value.asInstanceOf[Double])
               case IntegerType => point.addField(field.name, value.asInstanceOf[Int].toLong)
               case LongType => point.addField(field.name, value.asInstanceOf[Long])
               case _ => point.addTag(field.name, value.toString)
             }
           }
         }
      
         writeApi.writePoint(point)
       }
       logger.info(s"Successfully wrote ${dataFrame.count()} records to InfluxDB measurement: $measurement")
     } catch {
       case e: Exception =>
         logger.error(s"Error writing to InfluxDB: ${e.getMessage}", e)
     } finally {
       client.close()
    }
  }

  // def createGrafanaDashboard(): Unit = {
  //   logger.info("Creating Grafana dashboard...")
    
  //   val backend = HttpURLConnectionBackend()
  //   val grafanaUrl = config.getString("grafana.url")
  //   val grafanaApiKey = config.getString("grafana.api-key")
    
  //   val dashboardJson = Json.obj(
  //     "dashboard" -> Json.obj(
  //       "title" -> "IoT Analytics Dashboard",
  //       "tags" -> Json.arr("iot", "analytics"),
  //       "timezone" -> "browser",
  //       "panels" -> Json.arr(
  //         // Panel 1: Device Health Overview
  //         Json.obj(
  //           "id" -> 1,
  //           "title" -> "Device Health Status",
  //           "type" -> "stat",
  //           "targets" -> Json.arr(Json.obj(
  //             "query" -> "from(bucket: \"iot-analytics\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"device_health\")",
  //             "datasource" -> "InfluxDB"
  //           )),
  //           "gridPos" -> Json.obj("h" -> 8, "w" -> 12, "x" -> 0, "y" -> 0)
  //         ),
  //         // Panel 2: Geographic Temperature Distribution
  //         Json.obj(
  //           "id" -> 2,
  //           "title" -> "Geographic Temperature Distribution",
  //           "type" -> "geomap",
  //           "targets" -> Json.arr(Json.obj(
  //             "query" -> "from(bucket: \"iot-analytics\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"geographic_metrics\")",
  //             "datasource" -> "InfluxDB"
  //           )),
  //           "gridPos" -> Json.obj("h" -> 8, "w" -> 12, "x" -> 12, "y" -> 0)
  //         ),
  //         // Panel 3: Tank Level Alerts
  //         Json.obj(
  //           "id" -> 3,
  //           "title" -> "Tank Level Alerts",
  //           "type" -> "table",
  //           "targets" -> Json.arr(Json.obj(
  //             "query" -> "from(bucket: \"iot-analytics\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"tank_analytics\")",
  //             "datasource" -> "InfluxDB"
  //           )),
  //           "gridPos" -> Json.obj("h" -> 8, "w" -> 12, "x" -> 0, "y" -> 8)
  //         ),
  //         // Panel 4: Temporal Trends
  //         Json.obj(
  //           "id" -> 4,
  //           "title" -> "24-Hour Environmental Trends",
  //           "type" -> "timeseries",
  //           "targets" -> Json.arr(Json.obj(
  //             "query" -> "from(bucket: \"iot-analytics\") |> range(start: -24h) |> filter(fn: (r) => r._measurement == \"temporal_trends\")",
  //             "datasource" -> "InfluxDB"
  //           )),
  //           "gridPos" -> Json.obj("h" -> 8, "w" -> 12, "x" -> 12, "y" -> 8)
  //         )
  //       )
  //     ),
  //     "overwrite" -> true
  //   )
    
  //   val request = basicRequest
  //     .post(uri"$grafanaUrl/api/dashboards/db")
  //     .header("Authorization", s"Bearer $grafanaApiKey")
  //     .header("Content-Type", "application/json")
  //     .body(dashboardJson)
    
  //   Try(request.send(backend)) match {
  //     case Success(response) =>
  //       logger.info(s"Dashboard creation response: ${response.code}")
  //     case Failure(exception) =>
  //       logger.error(s"Failed to create dashboard: ${exception.getMessage}", exception)
  //   }
    
  //   backend.close()
  // }

  // def runAnalytics(): Unit = {
  //   logger.info("Starting IoT Analytics Service...")
    
  //   val spark = createSparkSession()
    
  //   try {
  //     // Load data from Minio
  //     val rawData = loadDataFromMinio(spark)
  //     logger.info(s"Loaded ${rawData.count()} events from Minio")
      
  //     // Run all analytics
  //     val deviceHealth = analyzeDeviceHealth(rawData)
  //     val geoDistribution = analyzeGeographicDistribution(rawData)
  //     val tankAnalytics = analyzeTankLevels(rawData)
  //     val temporalTrends = analyzeTemporalTrends(rawData)
      
  //     // Write results to InfluxDB
  //     writeToInfluxDB(deviceHealth, "device_health")
  //     writeToInfluxDB(geoDistribution, "geographic_metrics")
  //     writeToInfluxDB(tankAnalytics, "tank_analytics")
  //     writeToInfluxDB(temporalTrends, "temporal_trends")
      
  //     // Create Grafana dashboard
  //     createGrafanaDashboard()
      
  //     logger.info("Analytics completed successfully!")
      
  //   } catch {
  //     case e: Exception =>
  //       logger.error(s"Analytics failed: ${e.getMessage}", e)
  //   } finally {
  //     spark.stop()
  //   }
  // }
}

object Main extends App {
//   ServiceAnalytics.runAnalytics()
    // for the moment, just start a spark session to test the setup
    // val spark = SparkSession.builder()
    //   .appName("IoT Analytics Service")
    //   .master("local[*]")
    //   .getOrCreate()
    // create a Spark session
    val spark = ServiceAnalytics.createSparkSession()
    println("Spark session created successfully!")
    // load a json file from Minio
    val df = ServiceAnalytics.loadDataFromMinio(spark)
    println(s"Loaded ${df.count()} events from Minio")
    // show the first 5 rows in a file
    val flattenedDF = df
    .withColumn("lat", col("location.lat"))
    .withColumn("lon", col("location.lon"))
    .drop("location")

    // flattenedDF
    //     .coalesce(1) // reduce to a single partition (single output file)
    //     .write
    //     .mode("overwrite")
    //     .option("header", "true")
    //     .csv("output_single_file")

    // write to influxDB
    ServiceAnalytics.writeToInfluxDB(flattenedDF, "iot-analytics")
    println("Data written to InfluxDB measurement: iot-analytics")

    // analyze temporal trends
    val temporalTrendsDF = ServiceAnalytics.analyzeTemporalTrends(flattenedDF)
    println("Temporal trends analysis completed.")
    // write temporal trends to influxDB
    ServiceAnalytics.writeToInfluxDB(temporalTrendsDF, "temporal_trends")

    val deviceHealthDF = ServiceAnalytics.analyzeDeviceHealth(flattenedDF)
    println("Device health analysis completed.")
    // write device health to influxDB
    ServiceAnalytics.writeToInfluxDB(deviceHealthDF, "device_health")
    println("Device health data written to InfluxDB measurement: device_health")

    val geographicMetricsDF = ServiceAnalytics.analyzeGeographicDistribution(flattenedDF)
    println("Geographic distribution analysis completed.")
    // write geographic distribution to influxDB
    ServiceAnalytics.writeToInfluxDB(geographicMetricsDF, "geographic_metrics")
    println("Geographic metrics data written to InfluxDB measurement: geographic_metrics")

    val tankAnalyticsDF = ServiceAnalytics.analyzeTankLevels(flattenedDF)
    println("Tank level analysis completed.")
    // write tank analytics to influxDB
    ServiceAnalytics.writeToInfluxDB(tankAnalyticsDF, "tank_analytics")
    println("Tank analytics data written to InfluxDB measurement: tank_analytics")
    // write temporal trends to influxDB
    ServiceAnalytics.writeToInfluxDB(temporalTrendsDF, "temporal_trends")
    // println("Temporal trends data written to InfluxDB measurement: temporal_trends")

    println("Temporal trends data written to InfluxDB measurement: temporal_trends")
    // create a Grafana dashboard
    // ServiceAnalytics.createGrafanaDashboard()
    // println("Grafana dashboard created successfully.")


    // stop the Spark session
    spark.stop()
    println("Spark session stopped gracefully.")
}
