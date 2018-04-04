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

//    val userIDs =
//      users
//        .map(_._1)
//        .collect()
    val emptyEdgeRDD = RDD[Edge[Double]]()
    val relationships1 =
      userItemSet
        .cartesian(userItemSet)
        .filter(a => a._1._1 != a._2._1)
        .map(a => (a._1._1, a._2._1, a._1._2.intersect(a._2._2).size))
        .filter(_._3 >= 7)
        .cache()

    val relationships: RDD[Edge[Int]] =
      relationships1
        .union(relationships1.map(a => (a._2, a._1, a._3)))      //two directed edges for one undirected edge
        .map(a => Edge(a._1.toLong, a._2.toLong, 0))
    val defaultUser = -100
    // Build the initial Graph
    val initialGraph: Graph[Int, Int] =
      Graph(users, relationships, defaultUser)
        .cache()

    def vertexToBfsGraphFunFromGraph(graph: Graph[Int, Int], users: RDD[(VertexId, Int)]): ((VertexId, Int)) => Graph[Int, Double] = {
      def fun(a: (VertexId, Int)): Graph[Int, Double] = {
        var travelledSet = Set(a._1)
        var lastLevelSet = Set(a._1)
        var remainEdgeRDD: RDD[Edge[Double]] = RDD[Edge[Double]]()
        var vertexWeightMap =
          users
            .map(_._1)
            .collect()
            .map(a => (a, 0.0))
            .toMap
            .updated(a._1, 1.0)
        while (lastLevelSet.nonEmpty) {
          val thisLevelEdges =
            graph
              .mapEdges[Double](a => 0.0)
              .edges
            .filter(a => lastLevelSet.contains(a.srcId))
            .filter(a => !travelledSet.contains(a.dstId))
          lastLevelSet =
            thisLevelEdges
              .map(_.dstId)
              .collect()
              .toSet
          thisLevelEdges.foreach(a => {
            vertexWeightMap = vertexWeightMap.updated(
              a.dstId,
              vertexWeightMap.getOrElse(a.dstId, 0.0) + vertexWeightMap.getOrElse(a.srcId, 1.0)
            )
          })
          travelledSet = travelledSet.union(lastLevelSet)
          remainEdgeRDD = remainEdgeRDD.union(thisLevelEdges)
        }
        var bfsTreeGraph = Graph(users, remainEdgeRDD)
        var bottomUpLastSet = bfsTreeGraph.outDegrees.filter(_._2 == 0).map(_._1).collect().toSet
        val inDegreeMap = bfsTreeGraph.inDegrees.collect().toMap
        while (bottomUpLastSet.nonEmpty) {
          val thisEdges =
            bfsTreeGraph
              .edges
              .filter(a => bottomUpLastSet.contains(a.dstId))
              .map(a =>
                Edge(a.srcId,
                  a.dstId,
                  vertexWeightMap.getOrElse(a.dstId, 1.0) / inDegreeMap.getOrElse(a.dstId, 1).toDouble
                )
              )
          thisEdges
            .foreach(a =>
              vertexWeightMap =
                vertexWeightMap
                  .updated(
                    a.srcId,
                    vertexWeightMap.getOrElse(a.srcId, 1) + a.attr
                  )
            )
          bottomUpLastSet =
            thisEdges
              .map(_.srcId)
              .collect()
              .toSet
          bfsTreeGraph =
            Graph(
              bfsTreeGraph
                .vertices,
              bfsTreeGraph
                .edges
                .union(thisEdges)
            )
        }

        bfsTreeGraph.groupEdges((a, b) => a + b)
      }
      fun
    }
    var bfsGraphsCombined = Graph[Int, Double](initialGraph.vertices, emptyEdgeRDD)
    initialGraph
      .vertices
      .collect()
      .foreach(
        a => {
          val bfsGraphFromVertex = vertexToBfsGraphFunFromGraph(initialGraph, users)(a)
          bfsGraphsCombined = Graph(bfsGraphFromVertex.vertices, bfsGraphsCombined.edges.union(bfsGraphFromVertex.edges))
        }
      )
    val bfsGraphLargeToSmall =
      Graph(
        bfsGraphsCombined
          .vertices,
        bfsGraphsCombined
          .edges
          .filter(a => a.srcId > a.srcId)
      )
    bfsGraphsCombined =
      Graph(
        bfsGraphsCombined.vertices,
        bfsGraphsCombined
          .edges
          .filter(a => a.srcId <= a.dstId)
          .union(
            bfsGraphLargeToSmall
              .reverse
              .edges
          )
      )
        .groupEdges((a, b) => a + b)






  }
}


//use groupEdges in last step for betweenness