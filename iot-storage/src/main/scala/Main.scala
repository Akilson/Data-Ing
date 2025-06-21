import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json._
import scala.jdk.CollectionConverters._
import java.time.Duration
import java.util.Properties

case class IoTEvent(
  deviceId: String,
  timestamp: Long,
  temperature: Double,
  humidity: Double,
  pressure: Double,
  location: Location,
  status: String
)

case class Location(latitude: Double, longitude: Double)

object IoTEvent {
  implicit val locationReads: Reads[Location] = Json.reads[Location]
  implicit val iotEventReads: Reads[IoTEvent] = Json.reads[IoTEvent]
  implicit val locationWrites: Writes[Location] = Json.writes[Location]
  implicit val iotEventWrites: Writes[IoTEvent] = Json.writes[IoTEvent]
}

object IoTStorage {
  
  def createConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "iot-storage-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    
    new KafkaConsumer[String, String](props)
  }
  
  def createProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    
    new KafkaProducer[String, String](props)
  }
  
  def processEvent(event: IoTEvent, producer: KafkaProducer[String, String]): Unit = {
    try {
      // Store raw event in storage topic
      val storageRecord = new ProducerRecord[String, String](
        "iot-storage", 
        event.deviceId, 
        Json.toJson(event).toString()
      )
      producer.send(storageRecord)
      
      // Create enriched event with processing timestamp
      val enrichedEvent = Json.obj(
        "original" -> Json.toJson(event),
        "processedAt" -> System.currentTimeMillis(),
        "processingNode" -> "iot-storage-service"
      )
      
      // Store enriched event for analytics
      val analyticsRecord = new ProducerRecord[String, String](
        "iot-analytics", 
        event.deviceId, 
        enrichedEvent.toString()
      )
      producer.send(analyticsRecord)
      
      println(s"Stored event from device: ${event.deviceId} - Status: ${event.status}")
      
    } catch {
      case e: Exception =>
        println(s"Error processing event: ${e.getMessage}")
    }
  }
}

object Main extends App {
  println("IoT Storage Service starting...")
  
  val consumer = IoTStorage.createConsumer()
  val producer = IoTStorage.createProducer()
  
  // Subscribe to the IoT events topic
  consumer.subscribe(List("iot-events").asJava)
  
  println("Listening for IoT events...")
  
  try {
    while (true) {
      val records = consumer.poll(Duration.ofMillis(1000))
      
      records.asScala.foreach { record =>
        try {
          // Parse the IoT event
          val eventJson = Json.parse(record.value())
          eventJson.validate[IoTEvent] match {
            case JsSuccess(event, _) =>
              IoTStorage.processEvent(event, producer)
            case JsError(errors) =>
              println(s"Failed to parse event: $errors")
          }
        } catch {
          case e: Exception =>
            println(s"Error processing record: ${e.getMessage}")
        }
      }
    }
  } catch {
    case e: Exception =>
      println(s"Error in IoT Storage: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    consumer.close()
    producer.close()
  }
}
