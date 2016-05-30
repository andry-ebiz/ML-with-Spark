/**
  * Created by arandria on 11/04/2016.
  */

/* My imports */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object chapter1 {

  def main(args: Array[String]) {

    /* Define spark confs */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /* Create spark Context */
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("laposte_eboutique")

    implicit val sc = new org.apache.spark.SparkContext(conf)

    // Make an rdd made of a set of records of the form (user, product, price)
    val data: RDD[(String, String, String)] = sc.textFile("/Users/arandria/IdeaProjects/ML-with-Spark/data/UserPurchaseHistory.csv")
      .map(line => line.split(","))
      .map(purchasedRecord => (purchasedRecord(0), purchasedRecord(1), purchasedRecord(2)))

    // Count the total number of purchase
    val numPurchases: Long = data.count
    println("Total number of purchases is : " + numPurchases)

    // Unique number of users that made purchases
    val uniqueUsers: Long = data.map{case(user, product, price) => user}.distinct.count
    println("Total number of unique user is : " + uniqueUsers)

    // Compute the total revenu
    val totalRevenue: Double = data.map{case(user, product, price) => price.toDouble}.sum
    println("Total revenue is : " + totalRevenue)

    // Find the most popular product
    // - take negative values for sorting to avoid reversing the sort
    val productsByPopularity: Array[(String, Int)] = data
      .map{case(user, product, price) => (product, 1)}
      .reduceByKey(_ + _)
      .collect
      .sortBy(-_._2)

    val mostPopular: (String, Int) = productsByPopularity(0)
    productsByPopularity.foreach(println)
    println("Most popular product: %s with %d purchases".
      format(mostPopular._1, mostPopular._2))


  }

}
