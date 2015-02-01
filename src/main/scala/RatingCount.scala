import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.apache.spark.SparkContext._

import scala.util.Try

/**
 * Created by mwielocha on 01/02/15.
 */

case class Rating(dateTime: DateTime,
                  userId: Long,
                  entity: String,
                  value: Int)

object Rating {

  def fromCsv(csv: List[String]): Option[Rating] = {
    Try {
      Rating(
        DateTime.parse(csv(0)),
        csv(1).toLong,
        csv(2), csv(3).toInt)
    }.toOption
  }

  def avg(values: Iterable[Int]): Double = {
    values.sum.toDouble / values.size.toDouble
  }
}

trait RatingCount {
  requires: SparkApp =>

  val ratings: RDD[Rating] = sparkContext.textFile("ratings.csv.gz")
    .map(CsvParser.instance.parse)
    .flatMap(Rating.fromCsv)

  val byDayOfWeek = ratings.map {
    rating =>
      rating.dateTime.getDayOfWeek -> rating.value
  }.groupByKey()

  val result = byDayOfWeek.map {
    case (hour, values) =>
      hour -> Rating.avg(values)
  }

  result.collect().sortBy(_._2 * -1).foreach(println)

}
