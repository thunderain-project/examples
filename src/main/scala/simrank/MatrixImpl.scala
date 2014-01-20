package simrank

import com.esotericsoftware.kryo.Kryo

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.doubles.DoubleArrayList

import no.uib.cipr.matrix.DenseVector
import no.uib.cipr.matrix.sparse.SparseVector

import org.apache.spark.{InterruptibleIterator, Aggregator, Partitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{ExternalCombineByKeyRDD, RDD, ShuffledRDD}
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable

class MatrixImpl(@transient sc: SparkContext) extends ResultVerification {

  val graphPartitions = getVerifiedProperty("simrank.matrix.graphPartitions").toInt
  if (graphPartitions <= 1) {
    throw new IllegalArgumentException("graph partitions: " + graphPartitions
      + " should be larger than 1")
  }

  val externalCombine = getVerifiedProperty("simrank.matrix.useExternalCombine").toBoolean

  def createCombiner = (v: (Int, Double)) => {
    val idxs = new IntArrayList
    val vals = new DoubleArrayList
    idxs.add(v._1)
    vals.add(v._2)

    (idxs, vals)
  }

  def mergeValue = (c: (IntArrayList, DoubleArrayList), v: (Int, Double)) => {
    c._1.add(v._1)
    c._2.add(v._2)
    c
  }

  def mergeCombiner = (c1: (IntArrayList, DoubleArrayList), c2: (IntArrayList, DoubleArrayList)) => {
    c1._1.addElements(c1._1.size, c2._1.elements, 0, c2._1.elements.length)
    c1._2.addElements(c1._2.size, c2._2.elements, 0, c2._2.elements.length)
    c1
  }

  def adjacencyMatrix(): mutable.HashMap[(Int, Int), Double] = {
    val graphMap = new mutable.HashMap[Int, mutable.ArrayBuffer[(Int, Double)]]()

    val data = initializeGraphDataLocally(graphPath)
    (data.map(r => ((r._1._2, r._1._1), r._2)) ++ data).foreach { r =>
      val buf = graphMap.getOrElseUpdate(r._1._2, mutable.ArrayBuffer())
      buf += ((r._1._1, r._2))
    }

    graphMap.flatMap { kv =>
      val length = kv._2.length
      kv._2.map(e => ((e._1, kv._1), e._2 / length))
    }
  }

  def partitionAdjMatrix(adjMatrix: mutable.HashMap[(Int, Int), Double])
    : Array[mutable.ArrayBuffer[(Int, SparseVector)]] = {

    val transNormAdjCRSMatrix = new mutable.HashMap[Int, mutable.ArrayBuffer[(Int, Double)]]()
    adjMatrix.foreach { kv =>
      val buf = transNormAdjCRSMatrix.getOrElseUpdate(kv._1._1, mutable.ArrayBuffer())
      buf += ((kv._1._2, kv._2))
    }

    // partitions of adjacency matrix
    val adjMatArray =
      new Array[mutable.ArrayBuffer[(Int, SparseVector)]](graphPartitions)
    transNormAdjCRSMatrix.foreach { kv =>
      val key = kv._1 % graphPartitions
      val buf = Option(adjMatArray(key))
        .getOrElse {
        val m = mutable.ArrayBuffer[(Int, SparseVector)]()
        adjMatArray(key) = m
        m
      }
      val vec = new SparseVector(graphSize, 16)
      kv._2.foreach(r => vec.set(r._1, r._2))
      buf += ((kv._1, vec))
    }

    adjMatArray
  }

  def transpose(matrix: mutable.HashMap[(Int, Int), Double])
  : mutable.HashMap[(Int, Int), Double] = {
    matrix.map(e => ((e._1._2, e._1._1), e._2))
  }

  def initSimrankMatrix(): RDD[(Int, (IntArrayList, DoubleArrayList))] = {
    initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)
      .map(e => (e._1._2, (e._1._1, e._2)))
      .combineByKey(createCombiner, mergeValue, mergeCombiner, new ModPartitioner(partitions))
  }

  override def executeSimRank() {
    // transpose of normalized adjacency matrix, CRS sparse matrix
    val transNormAdjMatrix = transpose(adjacencyMatrix())
    val adjMatArray = partitionAdjMatrix(transNormAdjMatrix)
    val bdAdjMatArray = adjMatArray.map(m => sc.broadcast(m))

    // initial simrank matrix
    var simMatrix = initSimrankMatrix()

    (1 to iterations).foreach { i =>
      simMatrix = matrixSimrankCalculate(bdAdjMatArray, simMatrix, i)
    }

    //simMatrix.foreach(_ => Unit)

    /**
     * test for result verification, it should be disabled for real use
    val result = simMatrix.flatMap { p =>
      import scala.collection.JavaConversions._
      p._2._1.iterator zip p._2._2.iterator map(kv => ((p._1, kv._1.toInt), kv._2.toDouble))
    }.collect()
    assert(verify(result), "matrices comparison failed")
    */

  }

  protected def leftMatMult(
    iter1: Iterator[(Int, (IntArrayList, DoubleArrayList))],
    adjMatSlice: Broadcast[mutable.ArrayBuffer[(Int, SparseVector)]])
  : Iterator[(Int, (Int, Double))] = {

    iter1.flatMap { col =>
      val cIdx = col._1
      val column = new DenseVector(graphSize)
      import scala.collection.JavaConversions._
      col._2._1.iterator zip col._2._2.iterator foreach (kv => column.set(kv._1, kv._2))

      val slice = adjMatSlice.value
      slice.map { row =>
        val rIdx = row._1
        if (rIdx < graphASize && cIdx < graphASize ||
          rIdx >= graphASize && cIdx >= graphASize
        ) {
          (rIdx, (cIdx, 0.0))
        } else {
          val sum: Double = row._2.dot(column)
          (rIdx, (cIdx, sum))
        }
      }.filter(_._2._2 != 0.0)
    }
  }

  protected def rightMatMult(
    iter1: Iterator[(Int, (IntArrayList, DoubleArrayList))],
    adjMatSlices: Array[Broadcast[mutable.ArrayBuffer[(Int, SparseVector)]]])
  : Iterator[(Int, (IntArrayList, DoubleArrayList))] = {
    iter1.map { col =>
      val cIdx = col._1
      val column = new DenseVector(graphSize)
      import scala.collection.JavaConversions._
      col._2._1.iterator zip col._2._2.iterator foreach (kv => column.set(kv._1, kv._2))

      val idxs = new IntArrayList
      val vals = new DoubleArrayList

      adjMatSlices.flatMap { s =>
        val slice = s.value
        slice.map { row =>
          val rIdx = row._1
          if ((rIdx < graphASize && cIdx >= graphASize) ||
            (rIdx >= graphASize && cIdx < graphASize)) {
            (rIdx, 0.0)
          } else {
            if (rIdx == cIdx) {
              (rIdx, 1.0)
            } else {
              val sum: Double = row._2.dot(column)
              (rIdx, sum * 0.8)
            }
          }
        }
      }.filter(_._2 != 0.0).foreach(e => {idxs.add(e._1); vals.add(e._2)})

      (cIdx, (idxs, vals))
    }
  }

  def matrixSimrankCalculate(
    transAdjMatSlices: Array[Broadcast[mutable.ArrayBuffer[(Int, SparseVector)]]],
    simMatrix: RDD[(Int, (IntArrayList, DoubleArrayList))],
    iteration: Int)
  : RDD[(Int, (IntArrayList, DoubleArrayList))] = {

    val matArrays = for (slice <- transAdjMatSlices) yield {

      val aggregator = new Aggregator[Int, (Int, Double), (IntArrayList, DoubleArrayList)](
        createCombiner, mergeValue, mergeCombiner)

      val leftMatrix = simMatrix.mapPartitions { iter =>
        leftMatMult(iter, slice)
      }.mapPartitions(aggregator.combineValuesByKey, preservesPartitioning = true)

      val shuffleMatrix = new ShuffledRDD[
        Int,
        (IntArrayList, DoubleArrayList),
        (Int, (IntArrayList, DoubleArrayList))](leftMatrix, new SubModPartitioner(
        (partitions * scala.math.pow(2, iteration - 1)).toInt, graphPartitions))

      val leftMatColSlice  = if (externalCombine) {
        ExternalCombineByKeyRDD.combineByKey(shuffleMatrix,
          new SubModPartitioner(10, graphPartitions), aggregator)
      } else {
        shuffleMatrix.mapPartitionsWithContext((context, iter) => {
          new InterruptibleIterator(context, aggregator.combineCombinersByKey(iter))
        }, preservesPartitioning = true)
      }

      val rightMatColSlice = leftMatColSlice.mapPartitions { iter =>
        rightMatMult(iter, transAdjMatSlices)
      }

      rightMatColSlice
    }

    sc.union(matArrays)
  }
}

class MatrixElementKryoSerializer extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[(Int, (Int, Double))])
    kryo.register(classOf[(Int, (IntArrayList, DoubleArrayList))])
    kryo.register(classOf[IntArrayList])
    kryo.register(classOf[DoubleArrayList])
    kryo.register(classOf[((Int, Int), Double)])
    kryo.register(classOf[((Int, Int), (Double, Boolean))])
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

class SubModPartitioner(val partitions: Int, val subPartitions: Int) extends Partitioner {
  def numPartitions = partitions
  def getPartition(key: Any) = key.asInstanceOf[Int] / subPartitions % partitions

  override def equals(other: Any): Boolean = other match {
    case h: ModPartitioner =>
      h.numPartitions == numPartitions
    case _ => false
  }
}
