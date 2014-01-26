/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
