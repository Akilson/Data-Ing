import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.serialization.{StringDeserializer}
import java.util.Properties
import java.time.Duration
import scala.collection.JavaConverters._
import io.circe.parser.parse
import com.twilio.Twilio
import com.twilio.rest.api.v2010.account.Message
import com.twilio.`type`.PhoneNumber
import sttp.client3._
import sttp.model.MediaType
import io.circe.generic.auto._
import io.circe.syntax._

object Main extends App {
  println("Alert handler started")

  object SmsSender {
    val accountSid = sys.env("TWILIO_ACCOUNT_SID")     // or hardcode for now
    val authToken = sys.env("TWILIO_AUTH_TOKEN")        // or hardcode
    val fromNumber = sys.env("TWILIO_PHONE_NUMBER")                     // Your Twilio number
    val toNumber = sys.env("RECIPIENT_PHONE_NUMBER")

    def sendSms(body: String): Unit = {
      Twilio.init(accountSid, authToken)
      val message = Message
        .creator(
          new PhoneNumber(toNumber),
          new PhoneNumber(fromNumber),
          body
        )
        .create()

      println(s"✅ SMS sent! SID: ${message.getSid}")
    }
  }
  SmsSender.sendSms("Alert handler started") // Initial message to confirm Twilio setup
  object DiscordNotifierSev2 {

    // Replace with your Discord webhook URL
    val webhookUrl = "https://discord.com/api/webhooks/1389656695256514560/VvkjoM-EhZ9QFxIVIYrzrnY2X2aR-UjDzHmKbOG4p130thgGxr4suK1MAe-Qhb0-CNx9"

    case class DiscordMessage(content: String)

    def sendDiscordMessage(message: String): Unit = {
      val backend = HttpURLConnectionBackend()
      val discordMsg = DiscordMessage(message)

      val request = basicRequest
        .post(uri"$webhookUrl")
        .contentType(MediaType.ApplicationJson)
        .body(discordMsg.asJson.noSpaces)

      val response = request.send(backend)

      if (response.code.isSuccess) println("Alert sent to Discord!")
      else println(s"Failed to send Discord message: ${response.code} - ${response.body}")
    }
  }

  object DiscordNotifierSev3 {

    // Replace with your Discord webhook URL
    val webhookUrl = "https://discord.com/api/webhooks/1389659062328758323/BsJiIN-6E0iue0p0pspdW07xrXnD7qhrw7ouMQKINGKa5WzE_aDkjYFZuAVxlEqsan59"

    case class DiscordMessage(content: String)

    def sendDiscordMessage(message: String): Unit = {
      val backend = HttpURLConnectionBackend()
      val discordMsg = DiscordMessage(message)

      val request = basicRequest
        .post(uri"$webhookUrl")
        .contentType(MediaType.ApplicationJson)
        .body(discordMsg.asJson.noSpaces)

      val response = request.send(backend)

      if (response.code.isSuccess) println("Alert sent to Discord!")
      else println(s"Failed to send Discord message: ${response.code} - ${response.body}")
    }
  }

  def smsForSev1(message: String): String = {
    parse(message).toOption.flatMap { json =>
      val cursor = json.hcursor
      val temp = cursor.get[Double]("temperature").toOption
      val level = cursor.get[Int]("tankLevel").toOption
      val humidity = cursor.get[Double]("humidity").toOption
      val fermenter = cursor.get[String]("deviceId").toOption
      (temp, level, humidity, fermenter) match {
        case (Some(t), Some(l), Some(h), Some(f)) => Some("🔥 SEV1 Alert!: " + f + " - Temp: " + t + "°C, Level: " + l + "%, Humidity: " + h + "%")
        case _ => Some("")
      }
    }.getOrElse("")
  }

  def smsForSev23(message: String): String = {
    parse(message).toOption.flatMap { json =>
      val cursor = json.hcursor
      val temp = cursor.get[Double]("temperature").toOption
      val level = cursor.get[Int]("tankLevel").toOption
      val humidity = cursor.get[Double]("humidity").toOption
      val fermenter = cursor.get[String]("deviceId").toOption
      (temp, level, humidity, fermenter) match {
        case (Some(t), Some(l), Some(h), Some(f)) => Some(f + " - Temp: " + t + "°C, Level: " + l + "%, Humidity: " + h + "%")
        case _ => Some("")
      }
    }.getOrElse("")
  }
  val inputTopicSev1 = "alerts-sev1"
  val inputTopicSev2 = "alerts-sev2"
  val inputTopicSev3 = "alerts-sev3"
  
  // Consumer Setup
  val consumerProps: Properties = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-handler-group")

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(List(inputTopicSev1, inputTopicSev2, inputTopicSev3).asJava)

  def consumeLoop(): Unit = {
    val records = consumer.poll(Duration.ofMillis(100))
    records.asScala.foreach { record =>
      if (record.topic() == inputTopicSev1) {
        val messageToSend = smsForSev1(record.value())
        if (messageToSend == "") {
          println(s"Received SEV1 alert but could not parse: ${record.value()}")
          return
        }
        SmsSender.sendSms(messageToSend) // Replace with actual phone number
        println(s"Received SEV1 alert: ${record.value()}")
      } else if (record.topic() == inputTopicSev2) {
        val messageToSend = "Alert: " + smsForSev23(record.value())
        DiscordNotifierSev2.sendDiscordMessage(messageToSend)
        // println(s"Received SEV2 alert: ${record.value()}")
      } else if (record.topic() == inputTopicSev3) {
        val messageToSend = "Alert: " + smsForSev23(record.value())
        DiscordNotifierSev3.sendDiscordMessage(messageToSend)
        // println(s"Received SEV3 alert: ${record.value()}")
      } else {
        println(s"Received unknown alert from topic ${record.topic()}: ${record.value()}")
      }
    }
    consumeLoop()
  }
  consumeLoop()
}