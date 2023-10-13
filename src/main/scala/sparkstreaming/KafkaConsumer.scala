package sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

import java.util
import java.util.Properties
import java.util.regex.Pattern

object KafkaConsumer {

  // maintain the offset by hand. refresh offset -> calculate
  // maintain the offset automatically a. kafka -> __consumer_offset b. other places.
  // Auto_Offset_Reset_Config earliest: CURRENT_OFFSET  latest: LOG_END_OFFSET strategy when there are no offsets

  def main(args: Array[String]): Unit = {
    val pros = new Properties();
    pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node04:9092")
    pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer])
    pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer])

    pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"FALSE")

    pros.put(ConsumerConfig.GROUP_ID_CONFIG,"bula5")

    val consumer = new KafkaConsumer[String, String](pros)

    consumer.subscribe(Pattern.compile("00xx"),
      new ConsumerRebalanceListener {
        override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
          println(s"onPartitionsRevoked")
          val iter: util.Iterator[TopicPartition] = collection.iterator()
          while (iter.hasNext) {
            println(iter.next())
          }
        }

        override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
          println(s"onPartitionAssigned")

          val iter: util.Iterator[TopicPartition] = collection.iterator()
          while (iter.hasNext) {
            println(iter.next())
          }
          consumer.seek(new TopicPartition("00xx", 1), 46446)
          Thread.sleep(5000)
        }


        var record: ConsumerRecord[String, String] = null;

        while (true) {
          val records: ConsumerRecords[String, String] = consumer.poll(1000)

          if (!records.isEmpty) {

            val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()

            while (iter.hasNext) {
              record = iter.next()
              val topic: String = record.topic()
              val partition: Int = record.partition()
              val offset = record.offset()

              val key: String = record.key()
              val value: String = record.value()
              println(s"topic: $topic key:$key value:$value partition: $partition offset: $offset")
            }
          }
        }
        val partition = new TopicPartition("ooxx", record.partition())
        val offset = new OffsetAndMetadata(record.offset())

        val offMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
        offMap.put(partition, offset)
        consumer.commitSync(offMap)
      }
    )
  }
}
