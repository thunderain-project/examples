package simrank

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._

object ShufflePerformanceTest {

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: ShufflePerformanceTest sparkURL jarPath dataSize partitions")
      System.exit(-1)
    }

    val sc = new SparkContext(args(0), "ShufflePerformanceTest")
    sc.addJar(args(1))

    val dataSize = args(2).toInt
    val partitions = args(3).toInt

    val testMat = sc.parallelize(0 until dataSize, partitions).flatMap { r =>
      (0 until dataSize).map { i =>
        (r, (i, 1.0))
      }
    }

    // test read with write
    testMat.partitionBy(new HashPartitioner(partitions))
      .map(r => r)
      .partitionBy(new HashPartitioner(partitions))
      .foreach(_ => Unit)
  }
}
