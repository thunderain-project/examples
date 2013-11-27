package simrank

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class NaiveDeltaImpl(@transient sc: SparkContext) extends NaiveMapReduceImpl(sc) {

  val epsilon = getVerifiedProperty("simrank.delta.epsilon").toDouble

  override def executeSimRank(): Unit = {
    val graphData = initializeGraphDataLocally(graphPath)

    // broadcast graph data to each executor.
    val bdGraph = sc.broadcast(graphData)

    val initSimMatRDD = initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)
    var deltaSimMatRDD = simrankCalculate(bdGraph, initSimMatRDD)
    deltaSimMatRDD.persist(StorageLevel.DISK_ONLY).foreach(_ => Unit)

    var simMatRDD = initSimMatRDD.leftOuterJoin(deltaSimMatRDD).map { e =>
      (e._1, e._2._2.getOrElse(0.0) + e._2._1)
    }
    simMatRDD.persist(StorageLevel.DISK_ONLY).foreach(_ => Unit)

    // iterate to calculate the similarity matrix
    (0 until (iterations - 1)).foreach { i =>
      deltaSimMatRDD = deltaSimrankCalculate(bdGraph, deltaSimMatRDD)
      deltaSimMatRDD.persist(StorageLevel.DISK_ONLY)

      // S(t+1) = s(t) + delta(t+1)
      simMatRDD = simMatRDD.leftOuterJoin(deltaSimMatRDD).map { e =>
        (e._1, e._2._2.getOrElse(0.0) + e._2._1)
      }
      simMatRDD.persist(StorageLevel.DISK_ONLY)

      // trigger each step's action
      simMatRDD.foreach(_ => Unit)
    }

   simMatRDD.saveAsTextFile("result")
  }

  protected def deltaSimrankCalculate(bdGraph: Broadcast[Array[((Int, Int), Double)]],
    deltaSimMatRDD: RDD[((Int, Int), Double)]): RDD[((Int, Int), Double)] = {

    deltaSimMatRDD.filter { ele =>
      // filter out small value
      ele._1._1 != ele._1._2 && ele._2 > epsilon
    }.flatMap { ele =>
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
