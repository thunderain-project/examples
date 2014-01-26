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
