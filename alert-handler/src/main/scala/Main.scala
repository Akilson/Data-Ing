import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import play.api.libs.json._
import scala.jdk.CollectionConverters._
import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties

// create a kafka stream filter that filters out events with temperature > 30.0
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

  def processEvent(event: IoTEvent): Unit = {
    // Process the event (e.g., store it in a database or file)
    println(s"Processed event: $event")
  }
}
object Main extends App {
  println("IoT Storage starting...")

  val consumer = IoTStorage.createConsumer()
  consumer.subscribe(List("iot-events").asJava)

  println("Waiting for IoT events...")

  try {
    while (true) {
      val records = consumer.poll(Duration.ofMillis(1000))
      for (record <- records.asScala) {
        val eventJson = record.value()
        val event = Json.parse(eventJson).as[IoTEvent]

        // Filter out events with temperature > 30.0
        if (event.temperature <= 30.0) {
          IoTStorage.processEvent(event)
        } else {
          println(s"Filtered out event with high temperature: $event")
        }
      }
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    consumer.close()
  }
}