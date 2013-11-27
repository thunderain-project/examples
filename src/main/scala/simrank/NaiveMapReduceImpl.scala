package simrank

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class NaiveMapReduceImpl(@transient val sc: SparkContext)
  extends AbstractSimRankImpl {

  override def executeSimRank(): Unit = {
    val graphData = initializeGraphDataLocally(graphPath)

    // broadcast graph data to each executor.
    val bdGraph = sc.broadcast(graphData)

    val diagMatRDD = initializeData(getVerifiedProperty("simrank.diagSimMatPath"), sc)
    diagMatRDD.persist(StorageLevel.DISK_ONLY).foreach(_ => Unit)

    var simMatRDD = initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)

    // iterate to calculate the similarity matrix
    (1 to iterations).foreach { i =>
      simMatRDD = simrankCalculate(bdGraph, simMatRDD).union(diagMatRDD)
    }

    simMatRDD.saveAsTextFile("result")
  }

  def simrankCalculate(bdGraph: Broadcast[Array[((Int, Int), Double)]],
    simMatRDD: RDD[((Int, Int), Double)]): RDD[((Int, Int), Double)] = {

    /* TODO.
      1. zero value vertex can be filtered out.
      2. Change to MapPartitions.
      3. extract find out neighbors logic.
    */
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

      val outV1Len = outV1.length
      val outV2Len = outV2.length

      // get the cartesian product of outer neighbors
      for (u <- outV1; v <- outV2 if (u < v)) yield {
        if (v1 != v2) {
          ((u, v), (value * 0.8 / (outV1Len * outV2Len), true))
        } else {
          ((u, v), (value * 0.8 / (outV1Len * outV2Len), false))
        }
      }
    }.combineByKey[Double](
      (v: (Double, Boolean)) => if (v._2 == true) v._1 * 2 else v._1,
      (c: Double, v:(Double, Boolean)) => if (v._2 == true) c + v._1 * 2 else c + v._1,
      (c1: Double, c2: Double) => c1 + c2
    )
  }
}
