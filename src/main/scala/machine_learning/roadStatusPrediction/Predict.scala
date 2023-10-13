package machine_learning.roadStatusPrediction

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Predict {

  val sparkConf = new SparkConf().setAppName("predict traffic").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("Error")
  // create the date/time formatters
  val dayFormat = new SimpleDateFormat("yyyyMMdd")
  val minuteFormat = new SimpleDateFormat("HHmm")
  val sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val input = "2020-02-20_20:57:00"
    val date = sdf.parse(input)//yyyy-MM-dd_HH:mm:ss
    val inputTimeLong = date.getTime()
    val day = dayFormat.format(date)//yyyyMMdd

    // fetch data from redis
    val jedis = RedisClient.pool.getResource
    jedis.select(10)

    val camera_ids = List("310999003001", "310999003102")
    val camera_relations: Map[String, Array[String]] = Map[String, Array[String]](
      "310999003001" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204"),
      "310999003102" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204"))




  }
}
