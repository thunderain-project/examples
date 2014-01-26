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

import no.uib.cipr.matrix.DenseVector
import no.uib.cipr.matrix.sparse.SparseVector

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class PageRankLikeMatrixImpl(@transient sc: SparkContext) extends MatrixImpl(sc) {
  override def executeSimRank() {
    val adjMatrix = adjacencyMatrix()
    val adjMatArray = partitionAdjMatrix(adjMatrix)
    val bdMatArray = adjMatArray.map(sc.broadcast(_))

    var simMatrix = initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)
      .map(e => (e._1._2, (e._1._1, e._2)))
      .groupByKey(new HashPartitioner(partitions))
      .map { e =>
        val vec = new DenseVector(graphSize)
        e._2.foreach(r => vec.set(r._1, r._2))
        (e._1, vec)
      }

    (1 to iterations).foreach { i =>
      simMatrix = pageRankLikeMatrixSimrankCal(bdMatArray, simMatrix)
    }

    simMatrix.foreach(_ => Unit)

    //simMatrix.foreach { r =>
    //  import scala.collection.JavaConversions._
    //  r._2.iterator() foreach { e =>
    //    println(e.index(), r._1, e.get())
    //  }
    //}
  }

  def pageRankLikeMatrixSimrankCal(
    adjMatSlices: Array[Broadcast[Array[(Int, SparseVector)]]],
    simMatrix: RDD[(Int, DenseVector)]
  ): RDD[(Int, DenseVector)] = {
    simMatrix.mapPartitions { iter =>
      iter.map { col =>
        val cIdx = col._1
        val vec = new DenseVector(graphSize)

        adjMatSlices.flatMap { s =>
          val slice = s.value
          slice.map { row =>
            val rIdx = row._1
            val sum = row._2.dot(col._2)
            if (rIdx == cIdx) {
              (rIdx, sum * 0.8 + (1 - 0.8) * 1.0)
            } else {
              (rIdx, sum * 0.8)
            }
          }
        }.foreach(e => vec.set(e._1, e._2))

        (cIdx, vec)
      }
    }
  }
}
