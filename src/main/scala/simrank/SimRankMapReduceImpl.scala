package simrank

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

class SimRankMapReduceImpl(@transient val sc: SparkContext)
  extends AbstractSimRankImpl {

  val graphASize = getVerifiedProperty("simrank.mapreduce.graphA.size").toInt
  val graphBSize = getVerifiedProperty("simrank.mapreduce.graphB.size").toInt

  override def executeSimRank(): Unit = {
    val graphData = initializeGraphData(graphPath)

    // broadcast graph data to each executor.
    val bdGraph = sc.broadcast(graphData)

    // generate initial Identity Matrix as similarity matrix
    var simMatRDD = sc.parallelize{
      val simA = for (i <- (0 until graphASize); j <- (0 until graphASize) if (i <= j)) yield {
        val value = if (i != j) 0.0 else 1.0
        ((i.toLong, j.toLong), value)
      }

      val simB =  for {
        i <- (graphASize to graphSize - 1)
        j <- (graphASize to graphSize - 1)
        if (i <= j)
      } yield {
        val value = if (i != j) 0.0 else 1.0
        ((i.toLong, j.toLong), value)
      }

      simA ++ simB
    }

    // iterate to calculate the similarity matrix
    (1 to iterations).foreach{ i =>
      simMatRDD = simrankCalculate(bdGraph, simMatRDD)
      // trigger each step's action
      simMatRDD.foreach(println)
    }

   simMatRDD.saveAsTextFile("result")

  }

  private def simrankCalculate(bdGraph: Broadcast[Array[((Long, Long), Double)]],
    simMatRDD: RDD[((Long, Long), Double)]): RDD[((Long, Long), Double)] = {

    simMatRDD.flatMap { ele =>
      val((v1, v2), value) = ele
      val graph = bdGraph.value

      // find all the out neighbor of vertex v1
      val outV1 = if (v1 < graphASize && v2 < graphASize) {
        // fetch out the row number is v1
        graph.filter(e => (e._1._1) == v1).map(_._1._2)
      } else if (v1 >= graphASize && v2 >= graphASize) {
        // fetch out the colum number is v1
        graph.filter(e => (e._1._2) == v1).map(_._1._1)
      } else {
        throw new IllegalStateException(v1 + ", " + v2 + " is impossible to exist")
      }

      // find all the out neighbor of vertex v2
      val outV2 = if (v1 < graphASize && v2 < graphASize) {
        graph.filter(e => (e._1._1) == v2).map(_._1._2)
      } else if (v1 >= graphASize && v2 >= graphASize) {
        graph.filter(e => (e._1._2) == v2).map(_._1._1)
      } else {
        throw new IllegalStateException(v1 + ", " + v2 + " is impossible to exist")
      }

      // get the cartesian product of outer neighbors
      val neighbors = for (u <- outV1; v <- outV2 if (u <= v)) yield {
        ((u, v), value)
      }

      // since we only store the half of the matrix, so we should replicate the data for
      // the left-bottom half of the matrix
      if (v1 != v2) {
        neighbors ++ neighbors
      } else {
        neighbors
      }
    }.combineByKey[(Long, Double)](
      (v: Double) => (1l, v),
      (c: (Long, Double), v: Double) => (c._1 + 1, c._2 + v),
      (c1: (Long, Double), c2: (Long, Double)) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { ele =>
      if (ele._1._1 == ele._1._2) {
        (ele._1, 1.0)
      } else {
        (ele._1, ele._2._2 / ele._2._1 * 0.8)
      }
    }
  }
}
