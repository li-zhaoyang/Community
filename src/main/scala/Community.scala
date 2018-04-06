import java.io.{File, PrintWriter}

import Betweenness.result
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Community {
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X,Y)] = for { x <- xs; y <- ys } yield (x, y)
  }
  def modularity(graph: Graph[Int, Int], initialEdgeMap: Map[(Long, Long), Int]): Double = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
//    val m = graph.numEdges / 2
    val m = initialEdgeMap.keys.size / 2
    println(m)
    val outdegreeMap =
      graph
        .outDegrees
        .collect()
        .toMap
      graph
        .connectedComponents()
        .vertices
        .groupBy(_._2)
        .mapValues(a => a.map(_._1).toSet)
        .mapValues(a => (a cross a).toList.filter(b => b._1 != b._2))
        .mapValues(a => {
            val communitySize = a.size
            a.map(b => {
              - (outdegreeMap.getOrElse(b._1, 1) * outdegreeMap.getOrElse(b._2, 1)).toDouble / (2 * m).toDouble + initialEdgeMap.getOrElse((b._1, b._2), 0)
            }).sum
          })
        .values
        .sum() / (2 * m).toDouble
  }
  def main(args: Array[String]): Unit = {
    //arguments
    val inputFilePath: String = args(0)
    val outputDirPath: String = args(1)
    //get initial edge betweennesses from Betweenness.scala
    Betweenness.main(args)
    val initialSortedEdgeWeights = Betweenness
      .result
      .sortBy(_._2 * -1)
//    println(initialEdgeWeights)
//Threshold of common item # to form an edge between user
    val haveEdgeItemsNumThreshold = Betweenness.EDGE_THRESHOLD
    //spark
    val conf = new SparkConf()
    conf.setAppName("hw4")
    conf.setMaster("local[8]")
    val sc = SparkContext.getOrCreate(conf)

    val userItemSet: RDD[(Int, Set[Int])] =
      sc.textFile(inputFilePath)
        .filter(a => a!= "userID,productID, rating, timestamp")
        .map(a => a.split(','))
        .map(a => (a(0).toInt, a(1).toInt))
        .groupByKey()
        .mapValues(a => a.toSet)
        .cache()                          //all users and corresponding itemsets
    val usersSet =
      userItemSet
        .keys
        .collect()
        .map(_.toLong)
        .toSet                            //all users
    val vertices: RDD[(VertexId, Int)]=
      userItemSet
        .keys
        .map(a => (a.toLong, a))
        .cache()                            //vertices to form graph
    val relationships =
      userItemSet
        .cartesian(userItemSet)
        .filter(a => a._1._1 != a._2._1)
        .map(a => (a._1._1, a._2._1, a._1._2.intersect(a._2._2).size))
        .filter(_._3 >= haveEdgeItemsNumThreshold)
        .map(a => Edge(a._1.toLong, a._2.toLong, 0))
        .cache()                            //edgs to form graph
    val initialGraph: Graph[Int, Int] =
      Graph(vertices, relationships)
        .cache()                            // Build the initial Graph
    val edgeNum = initialGraph.edges.count().toInt / 2      //connected
    var thisGraph = initialGraph
    val edgeOneMap =
      relationships
        .map(a => ((a.srcId, a.dstId), 1))
        .collect()
        .toMap
    var maxMod = modularity(initialGraph, edgeOneMap)
    var index = -1
//    for (i <- 0 until edgeNum) {
//      thisGraph =
//        thisGraph
//          .subgraph(x => {
//            val a = initialSortedEdgeWeights(i)._1._1
//            val b = initialSortedEdgeWeights(i)._1._2
//            !(x.srcId == a && x.dstId == b) && !(x.srcId == b && x.dstId == a)
//          }
//      ).cache()
//      val thisMod = modularity(thisGraph, edgeOneMap)
//      if (thisMod > maxMod) {
//        maxMod = thisMod
//        index = i
//      }
//      println(i + ": " + thisMod)
//    }
    val takeOfEdgeSet = initialSortedEdgeWeights.take(50).map(_._1).toSet
    thisGraph =
      initialGraph
      .subgraph(x =>
                !takeOfEdgeSet.contains((x.srcId, x.dstId)) && !takeOfEdgeSet.contains((x.dstId, x.srcId))
              ).cache()
    val mod = modularity(thisGraph, edgeOneMap)
    println(mod)
    val communities =
      thisGraph
        .connectedComponents()
        .vertices
        .groupBy(_._2)
        .mapValues(a => a.map(_._1).toList.sorted)
        .values
        .collect()
        .sortBy(a => a.head)

    val writer = new PrintWriter(new File(outputDirPath + "Zhaoyang_Li_Community.txt"))
    communities
      .foreach(a => {
          writer.write("[")
          writer.write(a.head + "")
          a.tail.foreach(b => writer.write("," + b))
          writer.write("]\n")
        }
      )
    writer.close()

  }
}
