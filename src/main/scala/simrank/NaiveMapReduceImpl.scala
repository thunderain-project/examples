package simrank

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class NaiveMapReduceImpl(@transient val sc: SparkContext)
  extends AbstractSimRankImpl {
  val graphPartitions = getVerifiedProperty("simrank.partitions").toInt

  override def executeSimRank(): Unit = {
    val CRSGraph = new mutable.HashMap[Int, mutable.ArrayBuffer[Int]]()
    initializeGraphDataLocally(graphPath).foreach { r =>
      val buf = CRSGraph.getOrElseUpdate(r._1._1, mutable.ArrayBuffer())
      buf += r._1._2
    }

    val CCSGraph = new mutable.HashMap[Int, mutable.ArrayBuffer[Int]]()
    initializeGraphDataLocally(graphPath).foreach { r =>
      val buf = CCSGraph.getOrElseUpdate(r._1._2, mutable.ArrayBuffer())
      buf += r._1._1
    }

    // broadcast graph data to each executor.
    val bdCRSGraph = sc.broadcast(CRSGraph)
    val bdCCSGraph = sc.broadcast(CCSGraph)

    val diagMatRDD = initializeData(getVerifiedProperty("simrank.diagSimMatPath"), sc)
    diagMatRDD.persist(StorageLevel.DISK_ONLY).foreach(_ => Unit)

    var simMatRDD = initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)
      .partitionBy(new HashPartitioner(graphPartitions))

    // iterate to calculate the similarity matrix
    (1 to iterations).foreach { i =>
      simMatRDD = simrankCalculate(bdCRSGraph, bdCCSGraph, simMatRDD).union(diagMatRDD)
      simMatRDD.persist(StorageLevel.DISK_ONLY).foreach(_ => Unit)
    }

    //simMatRDD.saveAsTextFile("result")
  }

  def simrankCalculate(bdCRSGraph: Broadcast[mutable.HashMap[Int, mutable.ArrayBuffer[Int]]],
    bdCCSGraph: Broadcast[mutable.HashMap[Int, mutable.ArrayBuffer[Int]]],
    simMatRDD: RDD[((Int, Int), Double)]): RDD[((Int, Int), Double)] = {

    /* TODO.
      1. zero value vertex can be filtered out.
      2. Change to MapPartitions.
      3. extract find out neighbors logic.
    */
    simMatRDD.filter(e => e._2 != 0.0).flatMap { ele =>
      val((v1, v2), value) = ele
      val CRSGraph = bdCRSGraph.value
      val CCSGraph = bdCCSGraph.value

      // find all the out neighbor of vertex v1
      val outV1 = if (v1 < graphASize && v2 < graphASize) {
        // fetch out the row number is v1
        CRSGraph.getOrElse(v1, mutable.ArrayBuffer())
      } else if (v1 >= graphASize && v2 >= graphASize) {
        // fetch out the colum number is v1
        CCSGraph.getOrElse(v1, mutable.ArrayBuffer())
      } else {
        throw new IllegalStateException(v1 + ", " + v2 + " is impossible to exist")
      }

      // find all the out neighbor of vertex v2
      val outV2 = if (v1 < graphASize && v2 < graphASize) {
        CRSGraph.getOrElse(v2, mutable.ArrayBuffer())
      } else if (v1 >= graphASize && v2 >= graphASize) {
        CCSGraph.getOrElse(v2, mutable.ArrayBuffer())
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
