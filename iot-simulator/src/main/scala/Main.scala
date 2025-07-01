import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
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

  val kafkaProducerOption = 
    if (bootstrapServers.nonEmpty) Some(new KafkaProducer[String, String](props)) else None

  kafkaProducerOption.foreach { producer =>
    println(s"Successfully connected to Kafka. Publishing IoT events to topic: $topicName")

    val eventStream: LazyList[IoTEvent] = LazyList.continually(IoTSimulator.generateEvent())

    def sendEvents(events: LazyList[IoTEvent], sentCount: Int): Unit = {
      events.headOption.foreach { event =>
        val eventJson = Json.toJson(event).toString()
        val record = new ProducerRecord[String, String](topicName, event.deviceId, eventJson)

        producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
          if (exception != null) {
            println(s"Failed to send event: ${exception.getMessage}")
          } else {
            val newCount = sentCount + 1
            if (newCount % 10 == 0) {
              println(s"Successfully sent $newCount events. Latest: ${event.deviceId} - Temp: ${event.temperature}°C - Tank Level: ${event.tankLevel}%)")
            }
          }
        })

        Thread.sleep(1000 + new Random().nextInt(2000))
        sendEvents(events.tail, sentCount + 1)
      }
    }

    sendEvents(eventStream, 0)

    println("Closing Kafka producer...")
    producer.close()
    println("IoT Simulator shut down.")
  }

  if (kafkaProducerOption.isEmpty) {
    println("Kafka bootstrap servers not specified or empty, cannot create producer.")
  }
}