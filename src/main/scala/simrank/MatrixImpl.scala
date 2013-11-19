package simrank

/* test with Jblas and mahout math library*/
//import org.apache.mahout.math.{DenseMatrix, Matrix, SparseMatrix}

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

  def initSimrankMatrix(partitioner: Partitioner): RDD[((Int, Int), Double)] = {
    initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc).partitionBy(partitioner)
  }

  override def executeSimRank() {
    // tranpose of normalized adjacency matrix, CRS sparse matrix
    val transNormAdjMatrix = transpose(adjacencyMatrix())
      .map(e => (e._1._1, (e._1._2, e._2)))
      .groupByKey(new ModPartitioner(graphPartitions))
    transNormAdjMatrix.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    transNormAdjMatrix.foreach(_ => Unit)

    // initial simrank matrix
    var simMatrix = initSimrankMatrix(new ColumnPartitioner(partitions))

    (1 to iterations).foreach { i =>
      simMatrix = matrixSimrankCalculate(transNormAdjMatrix, simMatrix)
      //simMatrix.foreach(println)
    }

    simMatrix.saveAsTextFile("result")
  }

  def matrixSimrankCalculate(
    transAdjMatrix: RDD[(Int, Seq[(Int, Double)])],
    simMatrix: RDD[((Int, Int), Double)]): RDD[((Int, Int), Double)] = {

    def leftMatMult(
      iter1: Iterator[((Int, Int), Double)],
      iter2: Iterator[(Int, Seq[(Int, Double)])],
      part1: Int,
      part2: Int): Iterator[((Int, Int), Double)] = {
        val cols = (graphSize - 1 - part1) / partitions + 1
        val mat2 = DoubleMatrix.zeros(graphSize, cols)

        MatrixImpl.timeProfile("matrix Simrank matrix fill") {
          // it would be better to store in column oriented, for cache hit
          iter1.foreach(e => mat2.put(e._1._1, (e._1._2 - part1) / partitions, e._2))
        }

        iter2.flatMap { row =>
          val rIdx = row._1
          for (i <- 0 until mat2.columns) yield {
            val u = rIdx
            val v = i * partitions + part1
            if ((u < graphASize && v < graphASize) ||
              (u >= graphASize && v >= graphASize)) {
              ((v, u), 0.0)
            } else {
              var sum: Double = 0.0
              row._2.foreach(e => sum += e._2 * mat2.get(e._1, i))
              ((v, u), sum)
            }
          }
        }.filterNot(_._2 == 0.0)
    }

    def rightMatMult(
      iter1: Iterator[((Int, Int), Double)],
      iter2: Iterator[(Int, Seq[(Int, Double)])],
      part1: Int,
      part2: Int): Iterator[((Int, Int), Double)] = {
        val cols = (graphSize - 1 - part1) / partitions + 1
        val mat2 = DoubleMatrix.zeros(graphSize, cols)

        MatrixImpl.timeProfile("matrix Simrank matrix fill") {
          // it would be better to store in column oriented, for cache hit
          iter1.foreach(e => mat2.put(e._1._1, (e._1._2 - part1) / partitions, e._2))
        }

        iter2.flatMap { row =>
          val rIdx = row._1
          for (i <- 0 until mat2.columns) yield {
            val u = rIdx
            val v = i * partitions + part1
            if ((u < graphASize && v >= graphASize) ||
              (u >= graphASize && v < graphASize)) {
              ((v, u), 0.0)
            } else {
              var sum: Double = 0.0
              row._2.foreach(e => sum += e._2 * mat2.get(e._1, i))
              ((v, u), sum)
            }
          }
        }.filterNot(_._2 == 0.0)
    }
    //left two matrices multiplication
    val leftMatrix =
      CartesianPartitionsRDD.cartesianPartitions(simMatrix, transAdjMatrix, sc)(leftMatMult)
        .partitionBy(new ColumnPartitioner(partitions))

    //right two matrices multiplication
    val rightMatrix =
      CartesianPartitionsRDD.cartesianPartitions(leftMatrix, transAdjMatrix, sc)(rightMatMult)

    val resultMatrix = if (graphPartitions > 1) {
      rightMatrix.map(e => if (e._1._1 == e._1._2) (e._1, 1.0) else (e._1, e._2 * 0.8))
        .partitionBy(new ColumnPartitioner(partitions))
    } else {
      rightMatrix.map { e =>
        if (e._1._1 == e._1._2) ((e._1._2, e._1._1), 1.0)
        else ((e._1._2, e._1._1), e._2 * 0.8)
      }
    }

    resultMatrix
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

class ColumnPartitioner(val partitions: Int) extends Partitioner {
  def numPartitions = partitions
  def getPartition(key: Any) = key.asInstanceOf[(Int, Int)] match {
    case null => 0
    case (i, j) => j % partitions
  }

  override def equals(other: Any): Boolean = other match {
    case h: ColumnPartitioner =>
      h.numPartitions == numPartitions
    case _ => false
  }
}





