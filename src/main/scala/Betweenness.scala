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

    val haveEdgeItemsNumThreshold = 1

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

    initialGraph.edges.foreach(a => print("initial:" + a.srcId + "->" + a.dstId + ": " + a.attr + "\n"))

    def vertexToBfsGraphFunFromGraph(graph: Graph[Int, Int]): ((VertexId, Int)) => Graph[Int, Double] = {
      val users = graph.vertices
      def fun(thisVertex: (VertexId, Int)): Graph[Int, Double] = {
        var travelledSet = Set(thisVertex._1)
        var lastLevelSet = Set(thisVertex._1)
        var remainEdgeRDD: RDD[Edge[Double]] = emptyEdgeRDD
//        var bfsTreeEdgeSet = Set[(Long, Long)]()
        var vertexWeightMap =
          users
            .map(_._1)
            .collect()
            .map(a => (a, 0.0))
            .toMap
            .updated(thisVertex._1, 1.0)
        while (lastLevelSet.nonEmpty) {
          val thisLevelEdges =
            graph
              .edges
              .filter(a => lastLevelSet.contains(a.srcId) && !travelledSet.contains(a.dstId))
                .map(a => Edge(a.srcId, a.dstId, a.attr.toDouble))
                .cache()
//          bfsTreeEdgeSet = bfsTreeEdgeSet.union(thisLevelEdges.collect().map(a => (a.srcId, a.dstId)).toSet)
          lastLevelSet =
            thisLevelEdges
              .map(_.dstId)
              .collect()
              .toSet
          remainEdgeRDD = remainEdgeRDD.union(thisLevelEdges)
//          println("remainRDDSize: " + remainEdgeRDD.count())
//          thisLevelEdges
          thisLevelEdges.collect().foreach(a => {
            vertexWeightMap = vertexWeightMap.updated(
              a.dstId,
              vertexWeightMap.getOrElse(a.dstId, 0.0) + vertexWeightMap.getOrElse(a.srcId, 1.0)
            )
          })
          travelledSet = travelledSet.union(lastLevelSet)
        }
//        println(remainEdgeRDD.count())
//        remainEdgeRDD.foreach(a => print(thisVertex._1+"# vertex remainEdge: " + a.srcId + "->" + a.dstId + ": " + a.attr + "\n"))
        var bfsTreeGraph = Graph(users, remainEdgeRDD).partitionBy(PartitionStrategy.fromString("EdgePartition1D"))
        if (thisVertex._1 == 1L) {
          bfsTreeGraph.edges.foreach(a => print(a.srcId + "->" + a.dstId + ": " + a.attr))
        }

        val outDegrees = bfsTreeGraph.outDegrees.cache()
        val haveOutEdgeVerticeSet = outDegrees.map(_._1).cache().collect().toSet
//        outDegrees.foreach(a => println("vertex-" + thisVertex._1 + "-bfs-OutDegrees: " + a._1+ "-" + a._2))
        var bottomUpLastSet =
          graph
            .vertices
            .map(_._1)
            .collect()
            .toSet
          .diff(haveOutEdgeVerticeSet)
//        println("bottomUpLastSet Size:" + bottomUpLastSet.size)
        val inDegreeMap = bfsTreeGraph.inDegrees.cache().collect().toMap
        var outDegreeMap = outDegrees.collect().toMap
        if (thisVertex._1 == 4L) println("indegreeMap:" + inDegreeMap)
        if (thisVertex._1 == 4L) println("outdegreeMap:" + outDegreeMap)
        vertexWeightMap = users.collect().map(a => (a._1, 1.0)).toMap
        if (thisVertex._1 == 4L)  println("vertexWeightMap: " + vertexWeightMap)
//        breakable {
          while (bottomUpLastSet.nonEmpty) {
//            println("vertex:" + thisVertex +": " + bottomUpLastSet)
            var travelledEdgeSet = Set[Edge[Double]]()
            val thisEdges =
              bfsTreeGraph
                .edges
                .filter(a => bottomUpLastSet.contains(a.dstId))
                .map(a =>
                  Edge(
                    a.srcId,
                    a.dstId,
                    vertexWeightMap.getOrElse(a.dstId, 1.0) / inDegreeMap.getOrElse(a.dstId, 1).toDouble
                  )
                )
              if (thisVertex._1 == 4L) thisEdges.foreach(a => println("thisEdges:" + a.srcId + "->" + a.dstId + ":" + a.attr))
//            if (thisEdges.count() == 0) break
              travelledEdgeSet = travelledEdgeSet.union(thisEdges.collect().toSet)
            thisEdges
                .collect()
                .foreach(a => {
                  vertexWeightMap =
                    vertexWeightMap
                      .updated(
                        a.srcId,
                        vertexWeightMap.getOrElse(a.srcId, 1.0) + a.attr
                      )
                  outDegreeMap =
                    outDegreeMap
                      .updated(a.srcId, outDegreeMap.getOrElse(a.srcId, 1) - 1)
                  }
                )
            if (thisVertex._1 == 4L) println("outdegreeMap:" + outDegreeMap)
            bottomUpLastSet =
              thisEdges
                .map(_.srcId)
                .filter(a => outDegreeMap.getOrElse(a, 1) == 0)
                .collect()
                .toSet
            bfsTreeGraph =
              Graph(
                bfsTreeGraph
                  .vertices,
                bfsTreeGraph
                  .edges
                  .union(thisEdges)
              ).groupEdges((a, b) => a + b)
          }
//        }
        if (thisVertex._1 == 4L)
          bfsTreeGraph.edges.foreach(a => print("vertex-" + thisVertex._1 +"-bfsTree: "  + a.srcId + "->" + a.dstId + ": " + a.attr + "\n"))
        val ans = bfsTreeGraph.partitionBy(PartitionStrategy.fromString("EdgePartition1D")).groupEdges((a, b) => a + b)
//        println("I can see output in map!!!")
        ans
      }
      fun
    }
    println("hhhhhhhhhhhhhhhhhhhhhhhh")

    val bfsGraphs = initialGraph
      .vertices
      .collect()
      .map(vertexToBfsGraphFunFromGraph(initialGraph))
//    bfsGraphs.foreach(b => b.edges.foreach(a=> print("bfsTree: "  + a.srcId + "->" + a.dstId + ": " + a.attr + "\n")))
    var bfsGraphsCombined = bfsGraphs.reduce((a, b) => Graph(a.vertices, a.edges.union(b.edges)))
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

    bfsGraphsCombined.partitionBy(PartitionStrategy.fromString("EdgePartition1D")).groupEdges((a, b) => a + b).edges.foreach(a=> print("bfsTree: "  + a.srcId + "->" + a.dstId + ": " + a.attr + "\n"))









  }
}


//use groupEdges in last step for betweenness
//Thoughts: use Map[(Long,Long) -> Double] To represent the edges with weight during bfs. Each edge will only be used once.