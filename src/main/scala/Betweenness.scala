import java.io.{File, PrintWriter}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Betweenness {
  def EDGE_THRESHOLD = 7
  var result:Array[((Long, Long), Double)] = Array[((Long, Long), Double)]()
  def main(args: Array[String]): Unit = {
    //arguments
    val inputFilePath: String = args(0)
    val outputDirPath: String = args(1)
    //Threshold of common item # to form an edge between user
    val haveEdgeItemsNumThreshold = EDGE_THRESHOLD
    //spark
    val conf = new SparkConf()
    conf.setAppName("hw4")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

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
    //generate map function for distributed vertices from which we run bfs(modified) and compute betweenness in this bfs
    def vertexToBfsGraphFunFromGraph(users:Set[VertexId], allEdges: Set[Edge[Int]]):
      ((VertexId, Int)) => List[((Long, Long), Double)] = {
      def fun(thisVertex: (VertexId, Int)): List[((Long, Long), Double)]= {
        var travelledVertexSet = Set(thisVertex._1) //travelled edges ids in bfs, initialized with current root id
        var lastLevelVertexSet = Set(thisVertex._1) //current level to run bfs from
        var bfsTreeEdgeList = List[(Long, Long)]()  //edges of bfs travelled
        var vertexPathNumMap = Map[VertexId, Int]()
        var edgePathNumMap = Map[(VertexId, VertexId), Int]()
        vertexPathNumMap =
          vertexPathNumMap
            .updated(thisVertex._1, 1)
        while (lastLevelVertexSet.nonEmpty) {         //run bfs from root
          val thisLevelEdgesList =      //filter out edges of this level from all edges, filter by start vertex
            allEdges
              .filter(a => lastLevelVertexSet.contains(a.srcId) && !travelledVertexSet.contains(a.dstId))
              .map(a => (a.srcId, a.dstId))
              .toList
          bfsTreeEdgeList =
            bfsTreeEdgeList
              .union(thisLevelEdgesList)      //add this level to whole edge list
          thisLevelEdgesList
            .foreach(a => {
              edgePathNumMap =
                edgePathNumMap
                  .updated(
                    a,
                    vertexPathNumMap.getOrElse(a._1, 1)
                  )
              vertexPathNumMap =
                vertexPathNumMap
                  .updated(
                    a._2,
                    vertexPathNumMap.getOrElse(a._2, 0) + vertexPathNumMap.getOrElse(a._1, 1)
                  )
          })
          lastLevelVertexSet =
            thisLevelEdgesList
              .map(_._2)
              .toSet                          //update start vertices of bfs
          travelledVertexSet =
            travelledVertexSet
              .union(lastLevelVertexSet)    //update travelled vertices
        }
        var bfsEdgeWeightMap =
          bfsTreeEdgeList
            .map(a => (a, 0.0))
            .toMap                            //store weight of an edge in a map
        var outDegreeMap =
          bfsTreeEdgeList
            .groupBy(_._1)
            .mapValues(_.size)                //store outdegrees of vertices in a map
        val inDegreeMap =
          bfsTreeEdgeList
            .groupBy(_._2)
            .mapValues(_.size)                //store indegrees of vertices in a map
        val haveOutEdgeVerticeSet =
          outDegreeMap
            .keySet                            //the set of vertices that have edges out
        var bottomUpLastSet =
          travelledVertexSet
            .diff(haveOutEdgeVerticeSet)       //get leaves in bfs tree
        var vertexWeightMap = users
          .map(a => (a, 1.0))
          .toMap                                //store weight of a vertex in a map
        if (thisVertex._1 == 4L) {
          println(vertexPathNumMap)
          println(edgePathNumMap)
        }
        while (bottomUpLastSet.nonEmpty) {
            val thisEdges =
              bfsTreeEdgeList
                .filter(a => bottomUpLastSet.contains(a._2))          //run 'bfs' backwards
            thisEdges
              .foreach(a => {                               //update weight on each edges in this level of backward bfs
              bfsEdgeWeightMap = bfsEdgeWeightMap.updated(
                a,
                bfsEdgeWeightMap
                  .getOrElse(a, 0.0) + vertexWeightMap.getOrElse(a._2, 1.0) * vertexPathNumMap.getOrElse(a._1, 1).toDouble / vertexPathNumMap.getOrElse(a._2, 1).toDouble
              //give credit to edge as the fration of numbers of shortest path to dst going through this edge
              )
            })
            thisEdges
              .foreach(a => {                      //update weight on each starting vertex in this level of backward bfs
                vertexWeightMap =
                  vertexWeightMap
                    .updated(
                      a._1,
                      vertexWeightMap.getOrElse(a._1, 1.0) + bfsEdgeWeightMap.getOrElse(a, 0.0)
                    )
                //update outdegree map. we move up from a vertex only when all edges out of it have been travelled
                  outDegreeMap =
                    outDegreeMap
                      .updated(a._1, outDegreeMap.getOrElse(a._1, 1) - 1)
                  }
                )
            bottomUpLastSet =             //next level vertex of backward bfsa
              thisEdges
                .map(_._1)
                .filter(a => outDegreeMap.getOrElse(a, 1) == 0)   //remain only edges that all out edges are travelled
                .toSet
        }
        val ans = bfsEdgeWeightMap.toList
        ans
      }
      fun
    }
    val allEdges =
      initialGraph
        .collectEdges(EdgeDirection.Out)
        .flatMap(_._2)
        .collect()
        .toSet
    val bfsEdgesBetweenness =
      initialGraph
        .vertices
        .flatMap(vertexToBfsGraphFunFromGraph(usersSet, allEdges))     //flatmap to all bfs edges of betweennesses
        .map(a => {
          if (a._1._1 > a._1._2) ((a._1._2, a._1._1), a._2)           //make edge from small to large vertexId
          else a
        })
        .groupBy(_._1)
        .mapValues(a => a.map(_._2 / 2.0).sum)                          //get whole between of each edge
//    //print answer
//    bfsEdgesBetweenness
//      .foreach(a=> print("bfsTree: "  + a._1 + ": " + a._2 + "\n"))
//    println("count:" + bfsEdgesBetweenness.count())
    //output to file
    result = bfsEdgesBetweenness
      .collect()
      .sortBy(_._1._2)
      .sortBy(_._1._1)
    val writer = new PrintWriter(new File(outputDirPath + "Zhaoyang_Li_Betweenness.txt"))
    result
      .foreach(a =>
        writer.write("(" + a._1._1 + "," + a._1._2 + "," + a._2 + ")\n" )
      )
    writer.close()

  }
}

//Thoughts: use Map[(Long,Long) -> Double] To represent the edges with weight during bfs. Each edge will only be used once.