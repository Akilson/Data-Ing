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

  val inputTopic = "iot_events"
  val outputTopic = "alerts"
  
  // Consumer Setup
  val consumerProps: Properties = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-selector-group")

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(java.util.List.of(inputTopic))

  // Producer Setup
  val producerProps: Properties = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProps)

  // Filtering logic
  def isAlert(jsonStr: String): Boolean = {
    parse(jsonStr).toOption.flatMap { json =>
      val cursor = json.hcursor
      val temp = cursor.get[Double]("temperature").toOption
      val level = cursor.get[Int]("tank_level").toOption
      (temp, level) match {
        case (Some(t), Some(l)) => Some(t > 30.0 || l < 20)
        case _ => Some(false)
      }
    }.getOrElse(false)
  }

  println("Starting alert processor...")

  def consumeLoop(): Unit = {
    val records = consumer.poll(Duration.ofMillis(100))
    records.asScala.foreach { record =>
      if (isAlert(record.value())) {
        val outRecord = new ProducerRecord[String, String](outputTopic, record.key(), record.value())
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
