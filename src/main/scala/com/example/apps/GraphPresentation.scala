package com.example.apps

import org.apache.log4j.{Level, Logger}

object GraphPresentation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val vertices = IndexedSeq("Societe Generale","Credit Agricole","UBS","HSBC","BNP Paribas","RBS","Santander","Boursorama","Deutsche")
    val edges = Seq((0,1),(0,2),(1,3),(1,4),(2,5),(1,7),(3,6),(4,7),(5,8))
    val graph = new Graph(vertices,edges)
    val adjMatrix = graph.adjacencyMatrix
    for (i<-vertices){
      println(i + " --> " + (util.Try(adjMatrix(graph.index(i))) getOrElse("No connection")))
    }


  }
  class Graph(val vertex: IndexedSeq[String], edges: Seq[(Int, Int)]) {
    def size: Int = vertex.length
    val index: Map[String, Int] = vertex.zipWithIndex.toMap
    val adjacent = edges groupBy (_._1) mapValues (_ map (_._2))
    def adjacencyMatrix = adjacent mapValues (_.toSet) mapValues (0 to size map _)
  }

}
