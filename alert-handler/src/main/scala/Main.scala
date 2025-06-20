import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.serialization.{StringDeserializer}
import java.util.Properties
import java.time.Duration
import scala.collection.JavaConverters._
import io.circe.parser.parse

object Main extends App {
  println("Alert handler started")

  val inputTopic = "alerts"
  
  // Consumer Setup
  val consumerProps: Properties = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-selector-group")

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(java.util.List.of(inputTopic))

  def consumeLoop(): Unit = {
    val records = consumer.poll(Duration.ofMillis(100))
    records.asScala.foreach { record =>
    }
    consumeLoop()
  }

  consumeLoop()
}