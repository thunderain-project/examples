package simrank


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class DFSSimRankImpl(@transient sc: SparkContext) extends AbstractSimRankImpl {

  class MutableDouble(var value: Double)

  override def executeSimRank() {
    val graphData = initializeGraphDataLocally(graphPath)
    val bdGraph = sc.broadcast(graphData)

    val simMatRDD = initializeData(getVerifiedProperty("simrank.initSimMatPath"), sc)

    simMatRDD.filter(e => e._1._1 != e._1._2).mapPartitions { iter =>
      val graph = bdGraph.value
      iter.map { e =>
        val result = new MutableDouble(0.0)
        dfsSimRankCalculate(e._1, result, iterations - 1, graph)
        println(e._1._1 + " " + e._1._2 + ": " + result.value)
        (e._1, result.value)
      }
    }.foreach(_ => Unit)
  }

  def dfsSimRankCalculate(index: (Int, Int),
    value: MutableDouble,
    iter: Int,
    graph: Array[((Int, Int), Double)]) {

    val(v1, v2) = index
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

    // get the cartesian product of outer neighbors
    val outV1Len = outV1.length
    val outV2Len = outV2.length
    for (u <- outV1; v <- outV2) yield {
      if (u != v && iter != 0) {
        dfsSimRankCalculate((u, v), value, iter - 1, graph)
      } else if (u == v) {
        value.value += math.pow((0.8 /  (outV1Len * outV2Len)), iterations - iter)
      } else {
        value.value += 0.0
      }
    }
  }
}
