package sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer

import java.util


object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("kafka1")
    conf.set("spark.streaming.backpressure.enabled","true")
    conf.set("spark.streaming.kafka.maxRatePerPartition","2")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    val ssc = new StreamingContext(conf, Duration(1000))

    ssc.sparkContext.setLogLevel("ERROR")


    // get the dstream from kafka
    val map: Map[String, Object] = Map[String, Object] {
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node4"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"),  //需要手动维护offset 1，kafka    2，第三方
      (ConsumerConfig.GROUP_ID_CONFIG, "BULA666")
    }
    val inputStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, // distribute the data into several partitions equally.
      ConsumerStrategies.Subscribe[String, String](List("ooxx"), map)
    )

    val dstream: DStream[(String, (String, String, Int, Long))] = inputStream.map(
      record => {
        val t:String = record.topic()
        val p: Int = record.partition()
        val o: Long = record.offset()
        val k: String = record.key()
        val v: String = record.value()
        (k, (v, t, p, o))
      }
    )

    dstream.print()

    var ranges: Array[OffsetRange] = null
    inputStream.foreachRDD(
      rdd=>{
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //uses kafka to maintain the offset
        inputStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges, new OffsetCommitCallback {
          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
            if (offsets != null) {
              ranges.foreach(println)
              println("------------")
              val iter: util.Iterator[TopicPartition] = offsets.keySet().iterator()
              while (iter.hasNext) {
                val k: TopicPartition = iter.next()
                val v: OffsetAndMetadata = offsets.get(k)
                println(s"${k.partition()} ... ${v.offset()}")
              }
            }

          }
        })
        //save by mysql:start transaction, submit data, submit offset, commit
        val local: Array[(String, String)] = rdd.map(r=>(r.key(), r.value())).reduceByKey(_+_).collect()
      }
    )








  }


}
