package simrank

/* test with Jblas and mahout math library*/
//import org.apache.mahout.math.{DenseMatrix, Matrix, SparseMatrix}

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, CartesianPartitionsRDD}
import org.apache.spark.storage.StorageLevel

import org.jblas.DoubleMatrix

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
    // normalized adjacency matrix
    val normAdjMatrix = adjacencyMatrix().partitionBy(new ColumnPartitioner(graphPartitions))
    normAdjMatrix.persist(StorageLevel.MEMORY_AND_DISK)
    normAdjMatrix.foreach(_ => Unit)

    // tranpose of normalized adjacency matrix
    val transNormAdjMatrix = transpose(adjacencyMatrix())
      .partitionBy(new RowPartitioner(graphPartitions))
    transNormAdjMatrix.persist(StorageLevel.MEMORY_AND_DISK)
    transNormAdjMatrix.foreach(_ => Unit)

    // initial simrank matrix
    var simMatrix = initSimrankMatrix(new ColumnPartitioner(partitions))

    (1 to iterations).foreach { i =>
      simMatrix = matrixSimrankCalculate(normAdjMatrix, transNormAdjMatrix, simMatrix, i)
      //simMatrix.foreach(println)
    }

    simMatrix.saveAsTextFile("result")
  }

  def matrixSimrankCalculate(
    adjMatrix: RDD[((Int, Int), Double)],
    transAdjMatrix: RDD[((Int, Int), Double)],
    simMatrix: RDD[((Int, Int), Double)],
    iteration: Int): RDD[((Int, Int), Double)] = {

    //left two matrices multiplication
    val leftMatrix = CartesianPartitionsRDD.cartesianPartitions(simMatrix, transAdjMatrix, sc) {
      (iter1: Iterator[((Int, Int), Double)],
       iter2: Iterator[((Int, Int), Double)],
       part1: Int,
       part2: Int) =>
        val cols = (graphSize - 1 - part1) / partitions + 1
        val mat2 = if (iteration == 1) {
          //new SparseMatrix(graphSize, cols)
          DoubleMatrix.zeros(graphSize, cols)
        } else {
          //new DenseMatrix(graphSize, cols)
          DoubleMatrix.zeros(graphSize, cols)
        }

        MatrixImpl.timeProfile(" left matrix Simrank matrix fill") {
          //iter1.foreach(e => mat2.setQuick(e._1._1, (e._1._2 - part1) / partitions, e._2))
          iter1.foreach(e => mat2.put(e._1._1, (e._1._2 - part1) / partitions, e._2))
        }

        val rows = (graphSize - 1 - part2) / graphPartitions + 1
        //val mat1 = new SparseMatrix(rows, graphSize)
        val mat1 = DoubleMatrix.zeros(rows, graphSize)

        MatrixImpl.timeProfile("left matrix graph matrix fill") {
          //iter2.foreach( e => mat1.setQuick((e._1._1 - part2) / graphPartitions, e._1._2, e._2))
          iter2.foreach( e => mat1.put((e._1._1 - part2) / graphPartitions, e._1._2, e._2))
        }

        val multMat = MatrixImpl.timeProfile("left matrix multiplication") {
          //mat1.times(mat2)
          mat1.mmul(mat2)
        }

        //import scala.collection.JavaConversions._
        //multMat.iterator().flatMap { matSlice =>
        //  val idx = matSlice.index()
        //  val vecIter = matSlice.vector().nonZeroes().iterator()
        //  vecIter.map(e => ((idx * graphPartitions + part2, e.index * partitions + part1), e.get))
        //}
        {
          for (i <- 0 until multMat.rows; j <- 0 until multMat.columns if multMat.get(i, j) != 0.0)
            yield {
            ((i * graphPartitions + part2, j * partitions + part1), multMat.get(i, j))
          }
        }.toIterator
    }.partitionBy(new RowPartitioner(partitions))

    //right two matrices multiplication
    CartesianPartitionsRDD.cartesianPartitions(leftMatrix, adjMatrix, sc) {
      (iter1: Iterator[((Int, Int), Double)],
       iter2: Iterator[((Int, Int), Double)],
       part1: Int,
       part2: Int) =>
        val rows = (graphSize - 1 - part1) / partitions + 1
        val mat1 = if (iteration == 1) {
          //new SparseMatrix(rows, graphSize)
          DoubleMatrix.zeros(rows, graphSize)
        } else {
          //new DenseMatrix(rows, graphSize)
          DoubleMatrix.zeros(rows, graphSize)
        }

        MatrixImpl.timeProfile("right matrix simrank matrix fill") {
          //iter1.foreach(e => mat1.setQuick((e._1._1 - part1) / partitions, e._1._2, e._2))
          iter1.foreach(e => mat1.put((e._1._1 - part1) / partitions, e._1._2, e._2))
        }

        val cols = (graphSize - 1 - part2) / graphPartitions + 1
        //val mat2 = new SparseMatrix(graphSize, cols)
        val mat2 = DoubleMatrix.zeros(graphSize, cols)
        MatrixImpl.timeProfile("right matrix graph matrix fill") {
          //iter2.foreach(e => mat2.setQuick(e._1._1, (e._1._2 - part2) / graphPartitions, e._2))
          iter2.foreach(e => mat2.put(e._1._1, (e._1._2 - part2) / graphPartitions, e._2))
        }

        val multMat = MatrixImpl.timeProfile("right matrix multiplication") {
          //mat1.times(mat2)
          mat1.mmul(mat2)
        }

        //import scala.collection.JavaConversions._
        //multMat.iterator().flatMap { matSlice =>
        //  val idx = matSlice.index()
        //  val vecIter = matSlice.vector().nonZeroes().iterator()
        //vecIter.map { e =>
        //  val i = idx * partitions + part1
        //  val j = e.index * graphPartitions + part2
        //  if (i == j)
        //    ((i, j), 1.0)
        //  else
        //    ((i, j), e.get * 0.8)
        //}

        {
          for (i <- 0 until multMat.rows; j <- 0 until multMat.columns if multMat.get(i, j) != 0.0)
            yield {
            val u = i * partitions + part1
            val v = j * graphPartitions + part2
            if (u == v) ((u, v), 1.0)
            else ((u, v), 0.8 * multMat.get(i, j))
          }
        }.toIterator
      }.partitionBy(new ColumnPartitioner(partitions))
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

class RowPartitioner(val partitions: Int) extends Partitioner {
  def numPartitions = partitions
  def getPartition(key: Any) = key.asInstanceOf[(Int, Int)] match {
    case null => 0
    case (i, j) => i % partitions
  }

  override def equals(other: Any): Boolean = other match {
    case h: RowPartitioner =>
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





