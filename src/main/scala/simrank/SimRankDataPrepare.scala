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

import org.apache.spark.SparkContext

object SimRankDataPrepare {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("Usage: SimRankDataPrepare SparkMaster jarPath graphASize graphBSize " +
        "partitions simMatPath diagMatPath")
      System.exit(-1)
    }

    val sc = new SparkContext(args(0), "SimRankDataPrepare")
    sc.addJar(args(1))

    Utils.initSimMatCreate(args(2).toInt, args(3).toInt, args(4).toInt, args(5), sc)
    Utils.diagMatrixCreate(args(2).toInt + args(3).toInt, args(4).toInt, args(6), sc)
  }
}
