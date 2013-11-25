package simrank

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, CartesianPartitionsRDD}
import org.apache.spark.storage.StorageLevel

import org.jblas.DoubleMatrix
import org.apache.spark.serializer.KryoRegistrator

class MatrixImpl(@transient sc: SparkContext) extends AbstractSimRankImpl {

  val graphPartitions = getVerifiedProperty("simrank.matrix.graphPartitions").toInt

  def adjacencyMatrix(): RDD[((Int, Int), Double)] = {
    initializeData(graphPath, sc)
      .map( e => (e._1._2, (e._1._1, e._2)))
      .groupByKey()
      .flatMap { e =>
        val length = e._2.length
        e._2.map(r => ((r._1, e._1), r._2 / length))
      }
  }

  def transpose(matrix: RDD[((Int, Int), Double)]): RDD[((Int, Int), Double)] = {
    matrix.map(e => ((e._1._2, e._1._1), e._2))
  }

  def initSimrankMatrix(): RDD[((Int, Int), Double)] = {
    initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)
  }

  override def executeSimRank() {
    // tranpose of normalized adjacency matrix, CRS sparse matrix
    val transNormAdjMatrix = transpose(adjacencyMatrix())
      .map(e => (e._1._1, (e._1._2, e._2)))
      .groupByKey(new ModPartitioner(graphPartitions))
    transNormAdjMatrix.persist(StorageLevel.MEMORY_AND_DISK_2)
    transNormAdjMatrix.foreach(_ => Unit)

    // initial simrank matrix
    var simMatrix = initSimrankMatrix()
      .map(e => (e._1._2, (e._1._1, e._2)))
      .groupByKey(new ModPartitioner(partitions))

    (1 to iterations).foreach { i =>
      simMatrix = matrixSimrankCalculate(transNormAdjMatrix, simMatrix)
      //simMatrix.foreach(e => println(e._1 + ":" + e._2.mkString(" ")))
    }

    simMatrix.foreach(_ => Unit)
    //simMatrix.saveAsTextFile("result")
  }

  def matrixSimrankCalculate(
    transAdjMatrix: RDD[(Int, Seq[(Int, Double)])],
    simMatrix: RDD[(Int, Seq[(Int, Double)])]): RDD[(Int, Seq[(Int, Double)])] = {

    def leftMatMult(
      iter1: => Iterator[(Int, Seq[(Int, Double)])],
      iter2: => Iterator[(Int, Seq[(Int, Double)])],
      part1: Int,
      part2: Int): Iterator[(Int, (Int, Double))] = {
        iter1.flatMap { col =>
          val cIdx = col._1
          val column = new Array[Double](graphSize)
          col._2.foreach(e => column(e._1) = e._2)
          iter2.map { row =>
            val rIdx = row._1
            if ((rIdx < graphASize && cIdx < graphASize) ||
              (rIdx >= graphASize && cIdx >= graphASize)) {
              (rIdx, (cIdx, 0.0))
            } else {
              var sum: Double = 0.0
              row._2.foreach(e => sum += e._2 * column(e._1))
              (rIdx, (cIdx, sum))
            }
          }.filterNot(_._2._2 == 0.0)
        }
    }

    //left two matrices multiplication
    val leftMatrix =
      CartesianPartitionsRDD.cartesianPartitions(simMatrix, transAdjMatrix, sc)(leftMatMult)
      .groupByKey(new ModPartitioner(partitions))

    val rightMatrix = if (graphPartitions == 1) {
      def rightMatMult(
        iter1: => Iterator[(Int, Seq[(Int, Double)])],
        iter2: => Iterator[(Int, Seq[(Int, Double)])],
        part1: Int,
        part2: Int): Iterator[(Int, Seq[(Int, Double)])] = {
          iter1.map { col =>
            val cIdx = col._1
            val column = new Array[Double](graphSize)
            col._2.foreach(e => column(e._1) = e._2)
            val tmp = iter2.map { row =>
              val rIdx = row._1
              if ((rIdx < graphASize && cIdx >= graphASize) ||
                (rIdx >= graphASize && cIdx < graphASize)) {
                (rIdx, 0.0)
              } else {
                if (rIdx == cIdx) {
                  (rIdx, 1.0)
                } else {
                  var sum: Double = 0.0
                  row._2.foreach(e => sum += e._2 * column(e._1))
                  (rIdx, sum * 0.8)
                }
              }
            }.filterNot(_._2 == 0.0).toSeq
            (cIdx, tmp)
          }
      }

      CartesianPartitionsRDD.cartesianPartitions(leftMatrix, transAdjMatrix, sc)(rightMatMult)
   } else {
      def genericRightMatMult(
        iter1: => Iterator[(Int, Seq[(Int, Double)])],
        iter2: => Iterator[(Int, Seq[(Int, Double)])],
        part1: Int,
        part2: Int): Iterator[(Int, (Int, Double))] = {
          iter1.flatMap { col =>
            val cIdx = col._1
            val column = new Array[Double](graphSize)
            col._2.foreach(e => column(e._1) = e._2)
            iter2.map { row =>
              val rIdx = row._1
              if ((rIdx < graphASize && cIdx >= graphASize) ||
                (rIdx >= graphASize && cIdx < graphASize)) {
                (cIdx, (rIdx, 0.0))
              } else {
                if (rIdx == cIdx) {
                  (cIdx, (rIdx, 1.0))
                } else {
                  var sum: Double = 0.0
                  row._2.foreach(e => sum += e._2 * column(e._1))
                  (cIdx, (rIdx, sum * 0.8))
                }
              }
            }.filterNot(_._2._2 == 0.0)
          }
      }

      CartesianPartitionsRDD.cartesianPartitions(leftMatrix, transAdjMatrix, sc)(genericRightMatMult)
        .groupByKey(new ModPartitioner(partitions))
    }

    rightMatrix
  }
}

object MatrixImpl {
  def timeProfile[T](desc: String)(func: => T): T = {
    val start = System.currentTimeMillis()
    val ret = func
    val end = System.currentTimeMillis()
    println(desc + " time in millis: " + (end - start))
    ret
  }
}

class MatrixElementKryoSerializer extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[((Int, Int), Double)])
    kryo.register(classOf[(Int, Seq[(Int, Double)])])
  }
}

class ModPartitioner(val partitions: Int) extends Partitioner {
  def numPartitions = partitions
  def getPartition(key: Any) = key.asInstanceOf[Int] % partitions

  override def equals(other: Any): Boolean = other match {
    case h: ModPartitioner =>
      h.numPartitions == numPartitions
    case _ => false
  }
}





