package scala.algorithm

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.GraphLoader


/**
  * Created by pi on 7/31/17.
  */
class SocialCluster {

  var conf = new SparkConf().setAppName("Yelp Rating").setMaster("local[*]")
  var sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  def run()={
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "./src/data/input/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    cc.foreach(print(_))

//    // Join the connected components with the usernames
//    val users = sc.textFile("./src/data/input/users.txt").map { line =>
//      val fields = line.split(",")
//      (fields(0).toLong, fields(1))
//    }
//    val ccByUsername = users.join(cc).map {
//      case (id, (username, cc)) => (username, cc)
//    }
//    // Print the result
//    println(ccByUsername.collect().mkString("\n"))

  }
}
