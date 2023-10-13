package sparkstreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.concurrent.Future

object kafka_producer {
  def main(): Unit ={
    val pros = new Properties()
    pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node3:9092,node4:9092,node5:9092")
    pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])

    val producer = new KafkaProducer[String,String](pros)

    while (true) {
      for (i <- 1 to 3; j <- 1 to 3) {
        val record = new ProducerRecord[String, String]("ooxx", s"item$j", s"action$i")
        val records :Future[RecordMetadata] = producer.send(record)
        val metadata: RecordMetadata = records.get()
        val partition:Int = metadata.partition()
        val offset:Long = metadata.offset()

        println(s"item$j  action$i  partiton: $partition  offset: $offset")

        Thread.sleep(3000)


      }

    }
    producer.close()


  }

}
