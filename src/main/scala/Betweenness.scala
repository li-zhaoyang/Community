import org.apache.spark._
import org.apache.spark.graphx._
import scala.util.control.Breaks._
import org.apache.log4j.Logger
import org.apache.log4j.Level


import org.apache.spark.rdd.RDD
object Betweenness {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val inputFilePath: String = args(0)
    val outputDirPath: String = args(1)

    val haveEdgeItemsNumThreshold = 7

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
    val usersSet = userItemSet.keys.collect().map(_.toLong).toSet

    val users: RDD[(VertexId, Int)]=
      userItemSet
      .keys
      .map(a => (a.toLong, a))
      .cache()

//    val userIDs =
//      users
//        .map(_._1)
//        .collect()

    val emptyEdgeRDD = sc.parallelize( List[Edge[Double]]() )
    val relationships1 =
      userItemSet
        .cartesian(userItemSet)
        .filter(a => a._1._1 != a._2._1)
        .map(a => (a._1._1, a._2._1, a._1._2.intersect(a._2._2).size))
        .filter(_._3 >= haveEdgeItemsNumThreshold)
        .cache()

    val relationships: RDD[Edge[Int]] =
      relationships1
//        .union(relationships1.map(a => (a._2, a._1, a._3)))      //two directed edges for one undirected edge
        .map(a => Edge(a._1.toLong, a._2.toLong, 0))
    val defaultUser = -100
    // Build the initial Graph
    val initialGraph: Graph[Int, Int] =
      Graph(users, relationships, defaultUser)
        .cache()
//    initialGraph.edges.foreach(a => print("initial:" + a.srcId + "->" + a.dstId + ": " + a.attr + "\n"))

    def vertexToBfsGraphFunFromGraph(users:Set[VertexId], allEdges: Set[Edge[Int]]): ((VertexId, Int)) => List[((Long, Long), Double)] = {
//      val users = graph.vertices
      def fun(thisVertex: (VertexId, Int)): List[((Long, Long), Double)]= {
        var travelledSet = Set(thisVertex._1)
        var lastLevelSet = Set(thisVertex._1)
//        var remainEdgeRDD: RDD[Edge[Double]] = emptyEdgeRDD
        var bfsTreeEdgeList = List[(Long, Long)]()
        var vertexWeightMap =
          users
//            .map(_._1)
//            .collect()
            .map(a => (a, 0.0))
            .toMap
            .updated(thisVertex._1, 1.0)
        while (lastLevelSet.nonEmpty) {
          val thisLevelEdgesList =
            allEdges
              .filter(a => lastLevelSet.contains(a.srcId) && !travelledSet.contains(a.dstId))
              .map(a => (a.srcId, a.dstId))
//              .collect()
              .toList
          bfsTreeEdgeList = bfsTreeEdgeList.union(thisLevelEdgesList)
          lastLevelSet =
            thisLevelEdgesList
              .map(_._2)
              .toSet
          //          remainEdgeRDD = remainEdgeRDD.union(thisLevelEdges)
          //          println("remainRDDSize: " + remainEdgeRDD.count())
          //          thisLevelEdges
          //          thisLevelEdgesList.foreach(a => {
          //            vertexWeightMap = vertexWeightMap.updated(
          //              a._2,
          //              vertexWeightMap.getOrElse(a._2, 0.0) + vertexWeightMap.getOrElse(a._1, 1.0)
          //            )
          //          })
          travelledSet = travelledSet.union(lastLevelSet)
        }
        //        println(remainEdgeRDD.count())
        //        remainEdgeRDD.foreach(a => print(thisVertex._1+"# vertex remainEdge: " + a.srcId + "->" + a.dstId + ": " + a.attr + "\n"))
        //        var bfsTreeGraph = Graph(users, remainEdgeRDD).partitionBy(PartitionStrategy.fromString("EdgePartition1D"))

        var bfsEdgeWeightMap = bfsTreeEdgeList.map(a => (a, 0.0)).toMap
        var outDegreeMap = bfsTreeEdgeList.groupBy(_._1).mapValues(_.size)
        val haveOutEdgeVerticeSet = outDegreeMap.keySet
        //        outDegrees.foreach(a => println("vertex-" + thisVertex._1 + "-bfs-OutDegrees: " + a._1+ "-" + a._2))
        var bottomUpLastSet =
          travelledSet
            .diff(haveOutEdgeVerticeSet)
        //        println("bottomUpLastSet Size:" + bottomUpLastSet.size)
        val inDegreeMap =  bfsTreeEdgeList.groupBy(_._2).mapValues(_.size)
//        var outDegreeMap = outDegrees.collect().toMap
//        if (thisVertex._1 == 4L) println("indegreeMap:" + inDegreeMap)
//        if (thisVertex._1 == 4L) println("outdegreeMap:" + outDegreeMap)
        vertexWeightMap = users
//          .collect()
          .map(a => (a, 1.0)).toMap
//        if (thisVertex._1 == 4L)  println("vertexWeightMap: " + vertexWeightMap)
//        breakable {
          while (bottomUpLastSet.nonEmpty) {
//            println("vertex:" + thisVertex +": " + bottomUpLastSet)
//            var travelledEdgeSet = Set[Edge[Double]]()
            val thisEdges = bfsTreeEdgeList.filter(a => bottomUpLastSet.contains(a._2))
            thisEdges.foreach(a => {
              bfsEdgeWeightMap = bfsEdgeWeightMap.updated(
                a,
                bfsEdgeWeightMap.getOrElse(a, 0.0) + vertexWeightMap.getOrElse(a._2, 1.0) / inDegreeMap.getOrElse(a._2, 1).toDouble
              )
            })
//            val thisEdges =
//              bfsTreeGraph
//                .edges
//                .filter(a => bottomUpLastSet.contains(a.dstId))
//                .map(a =>
//                  Edge(
//                    a.srcId,
//                    a.dstId,
//                    vertexWeightMap.getOrElse(a.dstId, 1.0) / inDegreeMap.getOrElse(a.dstId, 1).toDouble
//                  )
//                )
//              if (thisVertex._1 == 4L) thisEdges.foreach(a => println("thisEdges:" + a._1+ "->" + a._2 + ":" + bfsEdgeWeightMap.getOrElse(a, 0.0)))
//            if (thisEdges.count() == 0) break
//              travelledEdgeSet = travelledEdgeSet.union(thisEdges.collect().toSet)
            thisEdges
                .foreach(a => {
                  vertexWeightMap =
                    vertexWeightMap
                      .updated(
                        a._1,
                        vertexWeightMap.getOrElse(a._1, 1.0) + bfsEdgeWeightMap.getOrElse(a, 0.0)
                      )
                  outDegreeMap =
                    outDegreeMap
                      .updated(a._1, outDegreeMap.getOrElse(a._1, 1) - 1)
                  }
                )
//            if (thisVertex._1 == 4L) println("outdegreeMap:" + outDegreeMap)
            bottomUpLastSet =
              thisEdges
                .map(_._1)
                .filter(a => outDegreeMap.getOrElse(a, 1) == 0)
                .toSet
//            bfsTreeGraph =
//              Graph(
//                bfsTreeGraph
//                  .vertices,
//                bfsTreeGraph
//                  .edges
//                  .union(thisEdges)
//              ).groupEdges((a, b) => a + b)
            bfsTreeEdgeList.union(thisEdges).distinct
          }
//        }
//        if (thisVertex._1 == 4L) bfsEdgeWeightMap.foreach(a => print("vertex-" + thisVertex._1 +"-bfsTree: "  + a._1._1 + "->" + a._1._2 + ": " + a._2 + "\n"))
        val ans = bfsEdgeWeightMap.toList
        //        val ans = bfsTreeGraph.partitionBy(PartitionStrategy.fromString("EdgePartition1D")).groupEdges((a, b) => a + b)
//        println("I can see output in map!!!")
        ans
      }
      fun
    }
    println("hhhhhhhhhhhhhhhhhhhhhhhh")
    val allEdges = initialGraph.collectEdges(EdgeDirection.Out).flatMap(_._2).collect().toSet
    val bfsEdgesWeight = initialGraph
      .vertices
//      .collect()
//      .toList
      .flatMap(vertexToBfsGraphFunFromGraph(usersSet, allEdges))
      .map(a => {
        if (a._1._1 > a._1._2) ((a._1._2, a._1._1), a._2)
        else a
      })
      .groupBy(_._1)
      .mapValues(a => a.map(_._2 / 2.0).sum)
    bfsEdgesWeight.foreach(a=> print("bfsTree: "  + a._1 + ": " + a._2 + "\n"))


//    bfsGraphs.foreach(b => b.edges.foreach(a=> print("bfsTree: "  + a.srcId + "->" + a.dstId + ": " + a.attr + "\n")))
//    var bfsGraphsCombined = bfsGraphs.reduce((a, b) => Graph(a.vertices, a.edges.union(b.edges)))
//    val bfsGraphLargeToSmall =
//      Graph(
//        bfsGraphsCombined
//          .vertices,
//        bfsGraphsCombined
//          .edges
//          .filter(a => a.srcId > a.srcId)
//      )
//    bfsGraphsCombined =
//      Graph(
//        bfsGraphsCombined.vertices,
//        bfsGraphsCombined
//          .edges
//          .filter(a => a.srcId <= a.dstId)
//          .union(
//            bfsGraphLargeToSmall
//              .reverse
//              .edges
//          )
//      )
//
//    bfsGraphsCombined.partitionBy(PartitionStrategy.fromString("EdgePartition1D")).groupEdges((a, b) => a + b).edges.foreach(a=> print("bfsTree: "  + a.srcId + "->" + a.dstId + ": " + a.attr + "\n"))
//








  }
}


//use groupEdges in last step for betweenness
//Thoughts: use Map[(Long,Long) -> Double] To represent the edges with weight during bfs. Each edge will only be used once.