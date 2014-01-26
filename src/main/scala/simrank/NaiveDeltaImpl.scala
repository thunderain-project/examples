/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package simrank

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class NaiveDeltaImpl(@transient sc: SparkContext) extends NaiveMapReduceImpl(sc) {

  val epsilon = getVerifiedProperty("simrank.delta.epsilon").toDouble

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


    val initSimMatRDD = initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)
      .partitionBy(new HashPartitioner(graphPartitions))

    var deltaSimMatRDD = simrankCalculate(bdCRSGraph, bdCCSGraph,  initSimMatRDD)
    deltaSimMatRDD.persist(StorageLevel.MEMORY_AND_DISK).foreach(_ => Unit)

    var simMatRDD = initSimMatRDD.leftOuterJoin(deltaSimMatRDD).map { e =>
      (e._1, e._2._2.getOrElse(0.0) + e._2._1)
    }
    simMatRDD.persist(StorageLevel.MEMORY_AND_DISK).foreach(_ => Unit)

    // iterate to calculate the similarity matrix
    (0 until (iterations - 1)).foreach { i =>
      deltaSimMatRDD = deltaSimrankCalculate(bdCRSGraph, bdCCSGraph,  deltaSimMatRDD)

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

  protected def deltaSimrankCalculate(
      bdCRSGraph: Broadcast[mutable.HashMap[Int, mutable.ArrayBuffer[Int]]],
      bdCCSGraph: Broadcast[mutable.HashMap[Int, mutable.ArrayBuffer[Int]]],
      deltaSimMatRDD: RDD[((Int, Int), Double)])
    : RDD[((Int, Int), Double)] = {

    deltaSimMatRDD.filter { ele =>
      // filter out small value
      ele._1._1 != ele._1._2 && ele._2 > epsilon
    }.flatMap { ele =>
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
          ((u, v), (value * 0.4 / (outV1Len * outV2Len), true))
        } else {
          ((u, v), (value * 0.4 / (outV1Len * outV2Len), false))
        }
      }
    }.combineByKey[Double](
      (v: (Double, Boolean)) => if (v._2 == true) v._1 * 2 else v._1,
      (c: Double, v:(Double, Boolean)) => if (v._2 == true) c + v._1 * 2 else c + v._1,
      (c1: Double, c2: Double) => c1 + c2
    )
  }
}
