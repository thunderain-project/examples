package simrank

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.{Aggregator, Partitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, CartesianPartitionsRDD, PartitionsRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable

class MatrixImpl(@transient sc: SparkContext) extends AbstractSimRankImpl {

  val graphPartitions = getVerifiedProperty("simrank.matrix.graphPartitions").toInt
  val subPartitions = getVerifiedProperty("simrank.matrix.subPartitions").toInt

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
    transNormAdjMatrix.persist(StorageLevel.MEMORY_AND_DISK_2).foreach(_ => Unit)

    // initial simrank matrix
    val simMatrix = initSimrankMatrix()
      .map(e => (e._1._2, (e._1._1, e._2)))
      .groupByKey(new ModPartitioner(partitions * subPartitions))

    var splitSimMatrices: Array[RDD[(Int, Seq[(Int, Double)])]] = {
      for (i <- 0 until subPartitions) yield {
        val parts = simMatrix.partitions.filter(_.index % subPartitions == i)
        new PartitionsRDD(simMatrix, parts)
      }
    }.toArray

    (1 to iterations).foreach { i =>
      splitSimMatrices = matrixSimrankCalculate(transNormAdjMatrix, splitSimMatrices)
    }

    splitSimMatrices.foreach(_.foreach(println))
    //splitSimMatrices.foreach(_.foreach(_ => Unit))
  }

  private def leftMatMult(
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


  def rightMatMult(
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

  def matrixSimrankCalculate(
    transAdjMatrix: RDD[(Int, Seq[(Int, Double)])],
    simMatrices: Array[_ <: RDD[(Int, Seq[(Int, Double)])]])
  : Array[RDD[(Int, Seq[(Int, Double)])]] = {

    val leftMatSlices = simMatrices.map { simMatSlice =>
      CartesianPartitionsRDD.cartesianPartitions(simMatSlice, transAdjMatrix, sc) {
        leftMatMult
      }.partitionBy(new ModPartitioner(partitions))
    }

    val leftMatrix = sc.union(leftMatSlices)

    val leftMatColSlices = {
      for (i <- 0 until subPartitions) yield {
        val parts = leftMatrix.partitions.filter(_.index % subPartitions == i)
        groupByKey(new PartitionsRDD(leftMatrix, parts))
      }
    }.toArray

    val rightMatColSlices = leftMatColSlices.map { leftMatColSlice =>
      CartesianPartitionsRDD.cartesianPartitions(leftMatColSlice, transAdjMatrix, sc) {
        rightMatMult
      }.groupByKey(new SubModPartitioner(partitions, subPartitions))
    }

    rightMatColSlices
  }

  private def groupByKey(rdd: RDD[(Int, (Int, Double))]): RDD[(Int, Seq[(Int, Double)])] = {
    def createCombiner(v: (Int, Double)) = mutable.ArrayBuffer(v)
    def mergeValue(buf: mutable.ArrayBuffer[(Int, Double)], v: (Int, Double)) = buf += v
    val aggregator = new Aggregator[Int, (Int, Double), mutable.ArrayBuffer[(Int, Double)]](
      createCombiner,
      mergeValue,
      null)

    val bufs = rdd.mapPartitions(aggregator.combineValuesByKey, preservesPartitioning = true)
    bufs.asInstanceOf[RDD[(Int, Seq[(Int, Double)])]]
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
    kryo.register(classOf[(Int, (Int, Double))])
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


class SubModPartitioner(val partitions: Int, val subPartitions: Int) extends Partitioner {
  def numPartitions = partitions
  def getPartition(key: Any) = key.asInstanceOf[Int] / subPartitions % partitions

  override def equals(other: Any): Boolean = other match {
    case h: ModPartitioner =>
      h.numPartitions == numPartitions
    case _ => false
  }
}