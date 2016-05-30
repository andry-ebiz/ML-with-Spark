
/**
  * Created by arandria on 23/05/2016.
  */

/* My packages */
package chapters

/* My imports */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.quantifind.charts.Highcharts._
import org.apache.spark.sql._
import scala.Predef
import scala.collection.{SeqView, Map}
import scala.util.Try

object SetConfs {

  /* --- Define spark confs --- */
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  /* --- Create spark Context --- */
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("MLWithSpark-book")

  implicit val sc = new org.apache.spark.SparkContext(conf)
  implicit val sqlsc = new org.apache.spark.sql.SQLContext(sc)

}

object Utils {

  /* Parse the dateRelease function */
  def parseDateRelease(d: String): Try[Int] = { Try(d.split("-")(2).toInt) }

}

object LoadDataSets {

  /* Import Spark Conf */
  import chapters.SetConfs._

  /* Construct case classes of data */
  case class User(id: String, age: Integer, gender: String, occupation: String, zipCode: String)
  case class Data(userid: Integer, itemid: Integer, rating: Double, t: BigInt)
  case class MovieDate(dateRelease: String, dateVideoRelease: String )


  /* Load text File */
  val uUser = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/ml-100k/u.user")
    .map(line => line.split('|'))
    .map(u => User(u(0), u(1).trim.toInt, u(2), u(3), u(4)))

  val uItem = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/ml-100k/u.item")
    .map(line => line.split('|'))

  val uData = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/ml-100k/u.data")
    .map(line => line.split("\t"))
    .map(r => Data(r(0).trim.toInt, r(1).trim.toInt, r(2).trim.toDouble, r(3).trim.toInt ))
}

object ExtractCategoricalFeatures {

  def main(args: Array[String]) {
    import chapters.LoadDataSets._

    // Collect all the possible states of the occupation variable
    val allOccupations = uUser.map{case(u: User) => u.occupation}.distinct.collect.sorted
    // allOccupations.foreach(println)

    val allOccupationsDict = allOccupations.zipWithIndex.toMap

    allOccupationsDict.foreach(println)
    println(allOccupationsDict("administrator"))




  }

}

object ExploreDBUser {

  def main(args: Array[String]) {
    import chapters.LoadDataSets._

    val numUsers = uUser.map(_.id).distinct.count
    val numGenders = uUser.map(_.gender).distinct.count
    val numOccupations = uUser.map(_.occupation).distinct.count
    val numZipCode = uUser.map(_.zipCode).distinct.count
    val numPerOccupation = uUser.map{case(u: User) => u.occupation}.countByValue

    /* Show results
    println(s"Summary: $numUsers Users, $numGenders Genders, $numOccupations Occupations, $numZipCode ZipCode"); println("")

    column(uUser.map{case (u: User) => (u.gender, 1)}.reduceByKey(_ + _).collect.map(_._2).toList); unhold
    histogram(uUser.map{case(u: User) => u.age.toInt}.collect.toList, 30)

    numPerOccupation.foreach(println); println("")
     */


  }
}

object ExploreDBItem {

  def main(args: Array[String]) {
    import chapters.LoadDataSets._
    import chapters.Utils._

    val moviesDates = uItem.map(m => MovieDate(m(2), m(3)))

    val mAges = moviesDates.map{case(d:MovieDate) => parseDateRelease(d.dateRelease).getOrElse(1900)}
      .filter(_ != 1900)
      .map(y => 2016 - y)
  }
}

object ExploreDBData {

  def main(args: Array[String]) {
    import chapters.LoadDataSets._

    val allRatings = uData.map{case(r:Data) => r.rating}
    val minR = allRatings.min
    val maxR = allRatings.max
    val meanR = allRatings.reduce(_ + _)

    // Compute the number of rating made by each user
    val distribRatingPerUser = uData.map({ case(r: Data) => (r.userid, 1) })
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)


    // Compute the number of rating given per movie
    val numRatingPerMovie = uData.map{case(r: Data) => (r.itemid, 1)}
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    /* Show results

    println("Minimal rating = " + minR); println("")
    println("Maximal rating = " + maxR); println("")
    println("Average rating = "  + meanR); println("")

    // Rather use the stats function
    println(allRatings.stats); println("")
    allRatings.countByValue.foreach(println); println("")

    distribRatingPerUser.take(10).foreach(println); println("")

    numRatingPerMovie.take(10).foreach(println); println("")

     */
  }
}




