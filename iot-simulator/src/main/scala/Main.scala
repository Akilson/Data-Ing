import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json._
import scala.util.{Random, Try, Success, Failure}
import java.time.Instant
import java.util.Properties

case class IoTEvent(
  deviceId: String,
  timestamp: String,
  location: Location,
  temperature: Double,
  humidity: Double,
  tankLevel: Int
)

case class Location(lat: Double, lon: Double)

object IoTEvent {
  implicit val locationWrites: Writes[Location] = Json.writes[Location]
  implicit val iotEventWrites: Writes[IoTEvent] = Json.writes[IoTEvent]
}

object IoTSimulator {
  private val random = new Random()
  
  // Fermenter device IDs
  private val deviceIds = Array(
    "fermenter-001", "fermenter-002", "fermenter-003",
    "fermenter-004", "fermenter-005", "fermenter-006"
  )

  def generateEvent(): IoTEvent = {
    val deviceId = deviceIds(random.nextInt(deviceIds.length))
    val timestamp = Instant.now().toString // ISO 8601 format
    
    // Generate realistic fermenter sensor data
    val temperature = 20.0 + random.nextDouble() * 15.0 // 20-35°C (fermentation range)
    val humidity = 50.0 + random.nextDouble() * 30.0 // 50-80% (controlled environment)
    val tankLevel = 10 + random.nextInt(91) // 10-100% tank level
    
    // Location coordinates around Bordeaux, France (wine region)
    val baseLat = 44.8381
    val baseLon = -0.5796
    val location = Location(
      lat = baseLat + (random.nextDouble() - 0.5) * 0.1,
      lon = baseLon + (random.nextDouble() - 0.5) * 0.1
    )

    IoTEvent(
      deviceId = deviceId,
      timestamp = timestamp,
      location = location,
      temperature = Math.round(temperature * 10.0) / 10.0, // Round to 1 decimal
      humidity = Math.round(humidity * 10.0) / 10.0, // Round to 1 decimal
      tankLevel = tankLevel
    )
  }
}

object Main extends App {
  println("IoT Simulator starting...")

  // Allow configuration via environment variables or system properties
  val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", 
    sys.props.getOrElse("kafka.bootstrap.servers", "localhost:9092"))
  
  println(s"Connecting to Kafka at: $bootstrapServers")

  // Kafka configuration
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  
  // Add some resilience settings
  props.put(ProducerConfig.RETRIES_CONFIG, "3")
  props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
  props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000")

  val topicName = "iot-events"

  // Try to create producer with better error handling
  Try(new KafkaProducer[String, String](props)) match {
    case Success(producer) =>
      println(s"Successfully connected to Kafka. Publishing IoT events to topic: $topicName")
      
      try {
        var eventCount = 0
        while (true) {
          // Generate and send an IoT event
          val event = IoTSimulator.generateEvent()
          val eventJson = Json.toJson(event).toString()
          val record = new ProducerRecord[String, String](topicName, event.deviceId, eventJson)
          
          // Send with callback for better error handling
          producer.send(record, (metadata, exception) => {
            if (exception != null) {
              println(s"Failed to send event: ${exception.getMessage}")
            } else {
              eventCount += 1
              if (eventCount % 10 == 0) {
                println(s"Successfully sent $eventCount events. Latest: ${event.deviceId} - Temp: ${event.temperature}°C")
              }
            }
          })
          
          // Wait 1-3 seconds before next event
          val random = new Random()
          Thread.sleep(1000 + random.nextInt(2000))
        }
      } catch {
        case e: InterruptedException =>
          println("Simulator interrupted. Shutting down gracefully...")
        case e: Exception =>
          println(s"Error in IoT Simulator: ${e.getMessage}")
          e.printStackTrace()
      } finally {
        println("Closing Kafka producer...")
        producer.close()
        println("IoT Simulator shut down.")
      }
      
    case Failure(exception) =>
      println(s"Failed to create Kafka producer: ${exception.getMessage}")
      println("\nTroubleshooting steps:")
      println("1. Make sure Kafka is running on the specified address")
      println("2. If using Docker, ensure the container is running and ports are exposed")
      println("3. Check network connectivity to the Kafka broker")
      println(s"4. Current bootstrap servers: $bootstrapServers")
  }
}