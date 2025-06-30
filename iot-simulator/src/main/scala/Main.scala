import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json._
import scala.util.Random
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Properties

case class IoTEvent(
  device_id: String,
  timestamp: String,
  location: Location,
  temperature: Double,
  humidity: Double,
  tank_level: Int
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
    val timestamp = Instant.now().format(DateTimeFormatter.ISO_INSTANT)
    
    // Generate realistic fermenter sensor data
    val temperature = 20.0 + random.nextDouble() * 15.0 // 20-35°C (fermentation range)
    val humidity = 50.0 + random.nextDouble() * 30.0    // 50-80% (controlled environment)
    val tankLevel = 10 + random.nextInt(91)             // 10-100% tank level
    
    // Location coordinates around Bordeaux, France (wine region)
    val baseLat = 44.8381
    val baseLon = -0.5796
    val location = Location(
      lat = baseLat + (random.nextDouble() - 0.5) * 0.1,
      lon = baseLon + (random.nextDouble() - 0.5) * 0.1
    )
    
    IoTEvent(
      device_id = deviceId,
      timestamp = timestamp,
      location = location,
      temperature = Math.round(temperature * 10.0) / 10.0, // Round to 1 decimal
      humidity = Math.round(humidity * 10.0) / 10.0,       // Round to 1 decimal
      tank_level = tankLevel
    )
  }
}

object Main extends App {
  println("IoT Simulator starting...")
  
  // Kafka configuration
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  
  val producer = new KafkaProducer[String, String](props)
  val topicName = "iot-events"
  
  println(s"Publishing IoT events to topic: $topicName")
  
  try {
    while (true) {
      // Generate and send an IoT event
      val event = IoTSimulator.generateEvent()
      val eventJson = Json.toJson(event).toString()
      
      val record = new ProducerRecord[String, String](topicName, event.deviceId, eventJson)
      producer.send(record)
      
      println(s"Sent event: ${event.deviceId} - ${event.status} - Temp: ${event.temperature}°C")
      
      // Wait 1-3 seconds before next event
      Thread.sleep(1000 + random.nextInt(2000))
    }
  } catch {
    case e: Exception =>
      println(s"Error in IoT Simulator: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    producer.close()
  }
}
