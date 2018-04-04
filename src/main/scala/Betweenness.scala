import org.apache.spark._
import org.apache.spark.graphx._

import org.apache.spark.rdd.RDD
object Betweenness {
  def main(args: Array[String]): Unit = {

    val inputFilePath: String = args(0)
    val outputDirPath: String = args(1)

    val conf = new SparkConf()
    conf.setAppName("hw4")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    val userItemSet =
      sc.textFile(inputFilePath)
        .filter(a => a!= "userID,productID, rating, timestamp")
        .map(a => a.split(','))
        .map(a => (a(0).toInt, a(1).toInt))
        .groupByKey()
//        .mapValues(a => a.size - a.toSet.size)
//        .filter(_._2 > 0)    //checked that no duplicate user item pairs
        .mapValues(a => a.toSet)
        .cache()

    val users: RDD[(VertexId, Int)]=
      userItemSet
      .keys
      .map(a => (a.toLong, a))

    val relationships1 =
      userItemSet
        .cartesian(userItemSet)
        .filter(a => a._1._1 != a._2._1)
        .map(a => (a._1._1, a._2._1, a._1._2.intersect(a._2._2).size))
        .filter(_._3 >= 7)
        .map(a => (a._1, a._2, 0))
        .cache()

    val relationships: RDD[Edge[(VertexId, Int)]] =
      relationships1
        .union(relationships1.map(a => (a._2, a._1, a._3)))      //two directed edges for one undirected edge
        .map(a => Edge(a._1.toLong, a._2.toLong, (-100L, a._3)))
    val defaultUser = -100
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)


  }
}
