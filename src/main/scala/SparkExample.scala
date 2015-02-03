import org.apache.spark.{SparkContext, SparkConf}
import System.nanoTime

import scala.concurrent.duration.Duration

object SparkExample extends App {

  def profile[R](units: Duration => Any)(function: => R): R = {
    val startTime = nanoTime
    val r = function
    val duration = Duration.fromNanos(nanoTime - startTime)
    println(s"Calculation took ${units(duration)} millis.")
    r
  }

  val s = "org.apache.spark.serializer.KryoSerializer"

  val sparkConf = new SparkConf()
    .setAppName("SparkApp")
    .setMaster("local")
    .set("spark.default.parallelism", "5")
    .set("spark.serializer", s)
    .registerKryoClasses(Array(classOf[Rating]))

  val sparkContext = new SparkContext(sparkConf)

  //new SparkApp(sparkContext) with WordCount1 - this throws Task not serializable: java.io.NotSerializableException

  new SparkApp(sparkContext) with WordCount2
  new SparkApp(sparkContext) with WordCount3
  new SparkApp(sparkContext) with WordCount4
  new SparkApp(sparkContext) with WordCount5

  profile(_.toMillis) {
    new SparkApp(sparkContext) with WordCount6
  }

  profile(_.toMillis) {
    new SparkApp(sparkContext) with RatingCount
  }
}
