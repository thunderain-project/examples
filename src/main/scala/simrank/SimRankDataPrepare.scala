package simrank

import org.apache.spark.SparkContext

object SimRankDataPrepare {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("Usage: SimRankDatPrepare SparkMaster jarPath graphASize graphBSize " +
        "partitions simMatPath diagMatPath")
      System.exit(-1)
    }

    val sc = new SparkContext(args(0), "SimRankDataPrepare")
    sc.addJar(args(1))

    Utils.initSimMatCreate(args(2).toInt, args(3).toInt, args(4).toInt, args(5), sc)
    Utils.diagMatrixCreate(args(2).toInt + args(3).toInt, args(4).toInt, args(6), sc)
  }
}
