import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import java.util.Properties
import java.time.Duration
import scala.collection.JavaConverters._
import io.circe.parser.parse
// import scala.jdk.CollectionConverters._

object Main extends App {
  println("Alert Selector started")

  val inputTopic = "iot-events"
  val outputTopicSev1 = "alerts-sev1"
  val outputTopicSev2 = "alerts-sev2"
  val outputTopicSev3 = "alerts-sev3"
  
  // Consumer Setup
  val consumerProps: Properties = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-selector-group")

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(java.util.List.of(inputTopic))

  // Producer Setup
  val producerProps: Properties = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProps)

  // Filtering logic
  def isAlert(jsonStr: String): Boolean = {
    parse(jsonStr).toOption.flatMap { json =>
      val cursor = json.hcursor
      val temp = cursor.get[Double]("temperature").toOption
      val level = cursor.get[Int]("tankLevel").toOption
      val humidity = cursor.get[Double]("humidity").toOption
      (temp, level, humidity) match {
        case (Some(t), Some(l), Some(h)) => Some(t > 30.0 || l < 35 || h > 65.0)
        case _ => Some(false)
      }
    }.getOrElse(false)
  }

  def Severity(jsonStr: String): String = {
    parse(jsonStr).toOption.flatMap { json =>
      val cursor = json.hcursor
      val temp = cursor.get[Double]("temperature").toOption
      val level = cursor.get[Int]("tankLevel").toOption
      val humidity = cursor.get[Double]("humidity").toOption
      (temp, level, humidity) match {
        case (Some(t), Some(l), Some(h)) => (t, l, h) match {
          // case (t, l, h) if t > 34.0 || l < 5 || h > 79 => Some(outputTopicSev1)
          case (t, l, h) if t > 32.5 || l < 20 || h > 70 => Some(outputTopicSev2)
          case _ => Some(outputTopicSev3)
        }
        // Should never enter in this case
        case _ => Some(outputTopicSev3)
      }
    }.getOrElse(outputTopicSev3)
  }

  println("Starting alert processor...")

  def consumeLoop(): Unit = {
    val records = consumer.poll(Duration.ofMillis(100))
    records.asScala.foreach { record =>
      if (isAlert(record.value())) {
        val severityTopic = Severity(record.value())
        val outRecord = new ProducerRecord[String, String](severityTopic, record.key(), record.value())
        producer.send(outRecord, (metadata, exception) => {
          if (exception != null) {
            println(s"Error sending alert: ${exception.getMessage}")
          } else {
            println(s"Alert sent successfully to topic ${metadata.topic()} partition ${metadata.partition()} offset ${metadata.offset()}")
          }
        })
      }
    }
    consumeLoop()
  }

  consumeLoop()
}
