package machine_learning.roadStatusPrediction

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable {
  val redisHost = "node1"
  val redisPort = 6379
  val redisTimeout = 30000
  // created when it used
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  lazy val hook = new Thread {
    override def run(): Unit = {
      println("Execute hook thread:" + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

}
