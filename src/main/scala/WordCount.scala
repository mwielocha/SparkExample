import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Created by mwielocha on 01/02/15.
 */
trait WordCount1 {
  requires: SparkApp =>

  val validWords: Set[String] = Set("memorable", "Webster", "broadened")

  val file: RDD[String] = sparkContext.textFile("input.txt")

  val result = file.flatMap(line => line.split(" "))
    .filter(validWords.contains)
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .collect()

  result.foreach(println)

}

object WordCount2 {

  val validWords: Set[String] = Set("memorable", "Webster", "broadened")

  val filterValidWords: String => Boolean = validWords.contains

}

trait WordCount2 {
  requires: SparkApp =>

  import WordCount2._

  val file: RDD[String] = sparkContext.textFile("input.txt")

  val result = file.flatMap(line => line.split(" "))
    .filter(filterValidWords)
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .collect()

  result.foreach(println)

}

trait WordCount3 {
  requires: SparkApp =>

  val validWords: Set[String] = Set("memorable", "Webster", "broadened")

  val file: RDD[String] = sparkContext.textFile("input.txt")

  def filter(rdd: RDD[String]): RDD[String] = {
    val validWordsLocal = validWords
    rdd.filter(validWordsLocal.contains)
  }

  val lines = file.flatMap(line => line.split(" "))

  val result = filter(lines)
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .collect()

  result.foreach(println)

}

trait WordCount4 {
  requires: SparkApp =>

  val validWords: Set[String] = Set("memorable", "Webster", "broadened")

  val file: RDD[String] = sparkContext.textFile("input.txt")

  val validWordsBroadcast = sparkContext.broadcast(validWords)

  val result = file.flatMap(line => line.split(" "))
    .filter(validWordsBroadcast.value.contains)
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .collect()

  result.foreach(println)

}

trait WordCount5 {
  requires: SparkApp =>

  val validWords: Seq[String] = Seq("memorable", "Webster", "broadened")

  val file: RDD[String] = sparkContext.textFile("input.txt")

  val validWordsRDD = sparkContext.parallelize(validWords)
    .keyBy(identity)

  val byWordRDD = file.flatMap(line => line.split(" "))
    .map(word => (word, 1))

  val result = validWordsRDD
    .join(byWordRDD)
    .map(_._2)
    .reduceByKey(_ + _)
    .collect()

  result.foreach(println)

}

trait WordCount6 {
  requires: SparkApp =>

  val dictionaryRDD = sparkContext
    .textFile("words.txt")
    .map(_.toLowerCase)
    .persist()

  val inputs: List[String] = List(
    "lotr1.txt", "lotr2.txt", "lotr3.txt",
    "hobbit.txt", "silmarillion.txt")

  inputs.foreach { input =>

    val inputRDD = sparkContext
      .textFile(input)
      .flatMap(_.split(" "))
      .map(_.replaceAll("[^a-zA-Z]", ""))
      .filter(_.length > 1)
      .map(_.toLowerCase)

    val result = inputRDD.subtract(dictionaryRDD)
      .map(word => word -> 1)
      .reduceByKey(_ + _)

    result.collect().sortBy(_._2 * -1)
      .zipWithIndex
      .foreach(x => println(s"$input: $x"))

  }
}
