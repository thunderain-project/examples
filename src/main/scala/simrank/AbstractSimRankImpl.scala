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

import java.io.{BufferedReader, FileReader, IOException}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

trait AbstractSimRankImpl extends Serializable {

  val graphPath = getVerifiedProperty("simrank.graph.data")
  val graphSize = getVerifiedProperty("simrank.graph.size").toInt
  val graphASize = getVerifiedProperty("simrank.graphA.size").toInt
  val graphBSize = getVerifiedProperty("simrank.graphB.size").toInt

  val iterations = getVerifiedProperty("simrank.iterations").toInt
  val partitions = getVerifiedProperty("simrank.partitions").toInt

  val regex = "\\(\\((\\d+),(\\d+)\\),(\\d+\\.\\d+)\\)".r

  def initializeData(path: String, sc: SparkContext): RDD[((Int, Int), Double)] = {
    sc.textFile(path).map { s =>
      val regex(i, j, v) = s
      ((i.toInt, j.toInt), v.toDouble)
    }
  }

   def getVerifiedProperty(key: String): String = {
    Option(SimRankImpl.getProp.getProperty(key)).getOrElse {
      throw new IOException(key + " should be set")
    }
  }

  def initializeGraphDataLocally(data: String): Array[((Int, Int), Double)] = {
    var is: BufferedReader = null
    try {
      is = new BufferedReader(new FileReader(data))

      val graph = new mutable.ArrayBuffer[((Int, Int), Double)]
      var line = is.readLine()
      while (line != null) {
        val regex(row, col, value) = line
        graph.append(((row.toInt, col.toInt), value.toDouble))

        line = is.readLine()
      }

      graph.toArray
    } catch {
      case e: Exception => throw new IOException("failed to read graph data")
    }
  }

  def executeSimRank(): Unit
}
