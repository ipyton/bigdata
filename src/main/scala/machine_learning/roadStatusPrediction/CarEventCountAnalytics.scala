package machine_learning.roadStatusPrediction

import net.sf.json.JSONObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer
import sun.java2d.pipe.SpanShapeRenderer.Simple

import java.text.SimpleDateFormat
import java.util.Calendar

object CarEventCountAnalytics   {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("")
    conf.setMaster("[*]") //set the threads according to the maximum cores
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = Set("car_events")
    val brokers = "node1:9092,node2:9092,node3:9092"
    // prepare for the kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "predictGroup",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val dbIndex = 10
    //create a direct stream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //receive the object and convert it
    val events: DStream[JSONObject] = kafkaStream.map(line =>{
      val data: JSONObject = JSONObject.fromObject(line.value())
      data
    })

    //carSpeed  K:monitor_id
    // 					V:(speedCount,carCount)
    //target: calculate the average number of speed to predict the future traffic situation
    val carSpeed : DStream[(String, (Int, Int))] =
      events.map(jb => (jb.getString("camera_id"), jb.getInt("speed"))).mapValues((speed:Int)=>(speed, 1))
        .reduceByKeyAndWindow((a:Tuple2[Int, Int], b:Tuple2[Int, Int]) => {(a._1 + b._1, a._2 + b._2)}, Seconds(60), Seconds(10)) //accumulate the results


    carSpeed.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val jedis = RedisClient.pool.getResource
        partitionOfRecords.foreach(pair => {
          val camera_id = pair._1
          val speedTotal = pair._2._1
          val carCount = pair._2._2

          val now = Calendar.getInstance().getTime()
          val dayFormat = new SimpleDateFormat("yyyyMMdd")
          val minuteFormat = new SimpleDateFormat("HHmm")
          val day = dayFormat.format(now)
          val time = minuteFormat.format(now)

          if (carCount != 0 && speedTotal != 0) {
            jedis.select(dbIndex)
            jedis.hset(day + "_" + camera_id, time, speedTotal + "_" + carCount)
          }
        })
        RedisClient.pool.returnResource(jedis)
      })
    })


  }
}
