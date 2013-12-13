package simrank

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.{Aggregator, Partitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, PartitionsRDD, SplitsRDD}
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable
import org.apache.spark.broadcast.Broadcast

class MatrixImpl(@transient sc: SparkContext) extends AbstractSimRankImpl {

  val graphPartitions = getVerifiedProperty("simrank.matrix.graphPartitions").toInt
  if (graphPartitions <= 1) {
    throw new IllegalArgumentException("graph partitions: " + graphPartitions
      + " should be larger than 1")
  }

  def adjacencyMatrix(): mutable.HashMap[(Int, Int), Double] = {
    val graphMap = new mutable.HashMap[Int, mutable.ArrayBuffer[(Int, Double)]]()
    initializeGraphDataLocally(graphPath).foreach { r =>
      val buf = graphMap.getOrElseUpdate(r._1._2, mutable.ArrayBuffer())
      buf += ((r._1._1, r._2))
    }

    graphMap.flatMap { kv =>
      val length = kv._2.length
      kv._2.map(e => ((e._1, kv._1), e._2 / length))
    }
  }

  def transpose(matrix: mutable.HashMap[(Int, Int), Double])
  : mutable.HashMap[(Int, Int), Double] = {
    matrix.map(e => ((e._1._2, e._1._1), e._2))
  }

  def initSimrankMatrix(): RDD[((Int, Int), Double)] = {
    initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)
  }

  override def executeSimRank() {
    // tranpose of normalized adjacency matrix, CRS sparse matrix
    val transNormAdjMatrix = transpose(adjacencyMatrix())
    val transNormAdjCRSMatrix = new mutable.HashMap[Int, mutable.ArrayBuffer[(Int, Double)]]()
    transNormAdjMatrix.foreach { kv =>
      val buf = transNormAdjCRSMatrix.getOrElseUpdate(kv._1._1, mutable.ArrayBuffer())
      buf += ((kv._1._2, kv._2))
    }

    // partitions of adjacency matrix
    val adjMatArray =
      new Array[mutable.ArrayBuffer[(Int, mutable.ArrayBuffer[(Int, Double)])]](graphPartitions)
    transNormAdjCRSMatrix.foreach { kv =>
      val key = kv._1 % graphPartitions
      val buf = Option(adjMatArray(key))
        .getOrElse {
        val m = mutable.ArrayBuffer[(Int, mutable.ArrayBuffer[(Int, Double)])]()
        adjMatArray(key) = m
        m
      }
      buf += ((kv))
    }

    val bdAdjMatArray = adjMatArray.map(m => sc.broadcast(m))

    // initial simrank matrix
    val simMatrix = initSimrankMatrix()
      .map(e => (e._1._2, (e._1._1, e._2)))
      .groupByKey(new ModPartitioner(partitions * graphPartitions))

    var splitSimMatrices: Array[RDD[(Int, Seq[(Int, Double)])]] = {
      for (i <- 0 until graphPartitions) yield {
        val parts = simMatrix.partitions.filter(_.index % graphPartitions == i)
        new PartitionsRDD(simMatrix, parts)
      }
    }.toArray

    (1 to iterations).foreach { i =>
      splitSimMatrices = matrixSimrankCalculate(bdAdjMatArray, splitSimMatrices, i)
      //splitSimMatrices.foreach( r => r.foreach(p => println(p._1 + ":" + p._2.mkString(" "))))
    }

    splitSimMatrices.foreach(r => r.foreach(_ => Unit))
    splitSimMatrices.foreach(r => r.foreach(_ => Unit))
    //splitSimMatrices.foreach(r => r.saveAsTextFile("result-" + r.id))
  }

  private def leftMatMult(
    iter1: Iterator[(Int, Seq[(Int, Double)])],
    adjMatSlice: Broadcast[mutable.ArrayBuffer[(Int, mutable.ArrayBuffer[(Int, Double)])]])
  : Iterator[(Int, (Int, Double))] = {

    iter1.flatMap { col =>
      val cIdx = col._1
      val column = new Array[Double](graphSize)
      col._2.foreach(e => column(e._1) = e._2)
      val slice = adjMatSlice.value
      slice.map { row =>
        val rIdx = row._1
        if (rIdx < graphASize && cIdx < graphASize ||
          rIdx >= graphASize && cIdx >= graphASize
        ) {
          (rIdx, (cIdx, 0.0))
        } else {
          var sum: Double = 0.0
          row._2.foreach(e => sum += e._2 * column(e._1))
          (rIdx, (cIdx, sum))
        }
      }.filter(_._2._2 != 0.0)
    }
  }

  def rightMatMult(
    iter1: Iterator[(Int, Seq[(Int, Double)])],
    adjMatSlices: Array[Broadcast[mutable.ArrayBuffer[(Int, mutable.ArrayBuffer[(Int, Double)])]]])
  : Iterator[(Int, Seq[(Int, Double)])] = {
    iter1.map { col =>
      val cIdx = col._1
      val column = new Array[Double](graphSize)
      col._2.foreach(e => column(e._1) = e._2)
      val iter = adjMatSlices.flatMap { s =>
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
              var sum: Double = 0.0
              row._2.foreach(e => sum += e._2 * column(e._1))
              (rIdx, sum * 0.8)
            }
          }
        }
      }.filter(_._2 != 0.0).toSeq
      (cIdx, iter)
    }
  }

  def matrixSimrankCalculate(
    transAdjMatSlices: Array[Broadcast[mutable.ArrayBuffer[(Int, mutable.ArrayBuffer[(Int, Double)])]]],
    simMatrices: Array[_ <: RDD[(Int, Seq[(Int, Double)])]],
    iteration: Int)
  : Array[RDD[(Int, Seq[(Int, Double)])]] = {

    for (slice <- transAdjMatSlices) yield {
      val leftMatSlices = simMatrices.map { simMatSlice =>
        simMatSlice.mapPartitions { iter =>
          leftMatMult(iter, slice)
        }
      }

      val leftMatrix = sc.union(leftMatSlices)
        .partitionBy(new SubModPartitioner(
          (partitions * scala.math.pow(2, iteration - 1)).toInt, graphPartitions))
      val leftMatColSlice  = groupByKey(leftMatrix)

      val rightMatColSlice = leftMatColSlice.mapPartitions { iter =>
        rightMatMult(iter, transAdjMatSlices)
      }

      rightMatColSlice
    }
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
