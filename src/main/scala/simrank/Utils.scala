package simrank

import org.apache.spark.SparkContext

object Utils extends Serializable {

  def initSimMatCreate(graphASize: Int,
    graphBSize: Int,
    partitions: Int,
    path: String,
    sc: SparkContext): Unit = {

    sc.parallelize(0 until partitions, partitions).flatMap { i =>
      val graphSize = graphASize + graphBSize
      val lines = graphSize / partitions

      val range = if (i == partitions - 1) {
        (i * lines, graphSize - 1)
      } else {
        (i * lines, (i + 1) * lines - 1)
      }

      if (range._1 < graphASize && range._2 < graphASize) {
        for (i <- range._1 to range._2; j <- i to graphASize - 1; if (i <= j)) yield {
          val value = if (i != j) 0.0 else 1.0
          ((i, j), value)
        }
      } else if (range._1 < graphASize && range._2 >= graphASize) {
        val simA =
          for (i <- range._1 to (graphASize - 1); j <- i to (graphASize -1) if (i <= j))
            yield {
              val value = if (i != j) 0.0 else 1.0
              ((i, j), value)
            }

        val simB =
          for (i <- graphASize to range._2; j <- i to graphSize - 1 if (i <= j))
            yield {
              val value = if (i != j) 0.0 else 1.0
              ((i, j), value)
            }

        simA ++ simB
      } else {
        for (i <- range._1 to range._2; j <- i to graphSize - 1; if (i <= j)) yield {
          val value = if (i != j) 0.0 else 1.0
          ((i, j), value)
        }
      }
    }.coalesce(partitions, true).saveAsTextFile(path)
  }

  def diagMatrixCreate(graphSize: Int, partitions: Int, path: String, sc: SparkContext): Unit = {
    sc.parallelize {
      for (i <- 0 until graphSize) yield {
        ((i, i), 1.0)
      }
    }.saveAsTextFile(path)
  }
}
