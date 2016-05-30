/**
  * Created by arandria on 11/04/2016.
  */

/* My imports */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.quantifind.charts.Highcharts._
import org.apache.spark.sql._

import scala.collection.Map
import scala.util.Try


/* MovieLens Dataset Description

u.data     -- The full u data set, 100000 ratings by 943 users on 1682 items.
              Each user has rated at least 20 movies.  Users and items are
              numbered consecutively from 1.  The data is randomly
              ordered. This is a tab separated list of
	         user id | item id | rating | timestamp.
              The time stamps are unix seconds since 1/1/1970 UTC

  196 242 3 881250949
  186 302 3 891717742
  22 377 1 878887116
  244 51 2 880606923
  166 346 1 886397596

u.info     -- The number of users, items, and ratings in the u data set.

u.item     -- Information about the items (movies); this is a tab separated
              list of
              movie id | movie title | release date | video release date |
              IMDb URL | unknown | Action | Adventure | Animation |
              Children's | Comedy | Crime | Documentary | Drama | Fantasy |
              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
              Thriller | War | Western |
              The last 19 fields are the genres, a 1 indicates the movie
              is of that genre, a 0 indicates it is not; movies can be in
              several genres at once.
              The movie ids are the ones used in the u.data data set.

  1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
  2|GoldenEye (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
  3|Four Rooms (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Four%20Rooms%20(1995)|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
  4|Get Shorty (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Get%20Shorty%20(1995)|0|1|0|0|0|1|0|0|1|0|0|0|0|0|0|0|0|0|0
  5|Copycat (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Copycat%20(1995)|0|0|0|0|0|0|1|0|1|0|0|0|0|0|0|0|1|0|0

u.genre    -- A list of the genres.

u.user     -- Demographic information about the users; this is a tab
              separated list of
              user id | age | gender | occupation | zip code
              The user ids are the ones used in the u.data data set.

  1|24|M|technician|85711
  2|53|F|other|94043
  3|23|M|writer|32067
  4|24|M|technician|43537
  5|33|F|other|15213

u.occupation -- A list of the occupations.

 */


object chapter3 {

  def WispPlot() = {
    /* Plotting Lines */
    val r = scala.util.Random
    val y1 = { for (i <- 0 to 10) yield r.nextInt(200) }
    val y2 = { for (i <- 0 to 10) yield r.nextInt(200) }
    val y3 = { for (i <- 0 to 10) yield r.nextInt(200) }

    line(0 to 10, y1); hold()
    line(0 to 10, y2); hold()
    line(0 to 10, y3); hold()

    title("Testing WIPS lines")
    xAxis("this is x axis label")
    yAxis("This is y axis label")
    legend(List("Line 1", "Line 2", "Line 3"))
  }

  def parseDateRelease(d: String): Try[Int] = {
    Try(d.split("-")(2).toInt)
  }


  def main(args: Array[String]) {

    /* Define spark confs */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /* Create spark Context */
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MLWithSpark-book")

    implicit val sc = new org.apache.spark.SparkContext(conf)
    implicit val sqlsc = new org.apache.spark.sql.SQLContext(sc)
    import sqlsc.implicits._


    /* -----------------------------------------------

    Explore USER dataset

    -------------------------------------------------- */
    case class User(id: String,
                    age: Integer,
                    gender: String,
                    occupation: String,
                    zipCode: String)

    val users = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/ml-100k/u.user")
      .map(line => line.split('|'))
      .map(u => User(u(0), u(1).trim.toInt, u(2), u(3), u(4)))

    val numUsers = users.map(_.id).distinct.count
    val numGenders = users.map(_.gender).distinct.count
    val numOccupations = users.map(_.occupation).distinct.count
    val numZipCode = users.map(_.zipCode).distinct.count
          // println(s"Summary: $numUsers Users, $numGenders Genders, $numOccupations Occupations, $numZipCode ZipCode")

          /* PLOT gender and age information */
            // column(users.map{case (u: User) => (u.gender, 1)}.reduceByKey(_ + _).collect.map(_._2).toList); unhold
            // histogram(users.map{case(u: User) => u.age.toInt}.collect.toList, 30)

    val numPerOccupation = users.map{case(u: User) => u.occupation}.countByValue
            // or use ReduceByKey users.map{case(u: userà => (u.occupation, 1)}.reduceByKey(_ + _)
            // numPerOccupation.foreach(println)



    /* -----------------------------------------------

    Explore MOVIE dataset

    -------------------------------------------------- */
    case class MovieDate(dateRelease: String,
                         dateVideoRelease: String )




    /* - - - - - - - - - - - */
    /* DO NOT USE CASE CLASS */
    /* - - - - - - - - - - - */
    // Get years of movies and plot the corresponding histogram

    val movies = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/ml-100k/u.item")
    val numMovies = movies.distinct.count
          // println(movies.first)

    val years: RDD[Int] = movies
      .map(line => line.split('|')(2))
      .map(sd => parseDateRelease(sd).getOrElse(1900))

    val yearsFiltered = years.filter(_ != 1900)

    val moviesAges = yearsFiltered.map(year => 2016 - year)
          // histogram(moviesAges.collect.toList, 100)


    /* - - - - - - - - - - - */
    /* USE ONLY CASE CLASS   */
    /* - - - - - - - - - - - */

    // Split the movies lines into arrays
    val moviesRDD: RDD[Array[String]] = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/ml-100k/u.item")
      .map(line => line.split('|'))

    // Make an rdd of dates and works with this file only
    val moviesDates: RDD[MovieDate] = moviesRDD.map(m => MovieDate(m(2), m(3)))
    val mAges: RDD[Int] = moviesDates.map{case(d:MovieDate) => parseDateRelease(d.dateRelease).getOrElse(1900)}
      .filter(_ != 1900).map(y => 2016 - y)

          // mAges.foreach(println)





    /* -----------------------------------------------

    Explore RATING dataset

    -------------------------------------------------- */

    case class MovieRate(userid: Integer,
                      itemid: Integer,
                      rating: Double,
                      t: BigInt)

    val ratingRDD: RDD[String] = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/ml-100k/u.data")
    val numR = ratingRDD.count

    val ccRatings = ratingRDD.map(line => line.split("\t"))
      .map(r => MovieRate(r(0).trim.toInt, r(1).trim.toInt, r(2).trim.toDouble, r(3).trim.toInt ))

    val allRatings = ccRatings.map{case(r:MovieRate) => r.rating}
    val minR = allRatings.min
    val maxR = allRatings.max
    val meanR = allRatings.reduce(_ + _) / numR
    val numRperMov = numR / numMovies
    val numRperUsr = numR / numUsers

        // println("Number of ratings = " + numR)
        // println("Minimal rating = " + minR)
        // println("Maximal rating = " + maxR)
        // println("Average rating = "  + meanR)
        // println("Number of rating per movie = " + numRperMov)
        // println("Number of rating per user = " + numRperUsr)

    // Rather use the stats function
        // println(allRatings.stats)
        // allRatings.countByValue.foreach(println)

    // Compute the number of rating made by each user
    val distribRatingPerUser = ccRatings.map({ case(r: MovieRate) => (r.userid, 1) })
      .reduceByKey(_ + _)
      .mapValues(x =>  x.toDouble * 100 / numR)
      .sortBy(_._2, ascending = false)

    distribRatingPerUser.take(10).foreach(println); println("")

    // Compute the number of rating given per movie
    val numRatingPerMovie = ccRatings.map{case(r: MovieRate) => (r.itemid, 1)}
      .reduceByKey(_ + _).mapValues(_.toDouble * 100 / numR)
      .sortBy(_._2, ascending = false)

    numRatingPerMovie.take(10).foreach(println)





  }

}
