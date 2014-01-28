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

import java.io.{FileInputStream, IOException}
import java.util.Properties

import org.apache.spark.SparkContext

object SimRankImpl {

  private var properties: Properties = _
  def getProp = properties

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: SimRankImpl config/config.properies")
      System.exit(-1)
    }

    properties = new Properties()
    properties.load(new FileInputStream(args(0)))

    // Create Application logic
    val master = properties.getProperty("spark.master.url", "local[10]")
    val appName = properties.getProperty("spark.app.name", "SimRank")
    val sc = new SparkContext(master, appName)

    Option(properties.getProperty("spark.app.jars")).foreach { r =>
      val jars = r.split(",")
      jars.foreach(j => sc.addJar(j))
    }

    Option(properties.getProperty("simrank.impl.type")).map { r =>
      val typ = SimRankImplType.withName(r)
      val simrankImpl = typ match {
        case SimRankImplType.mapreduce =>
          new NaiveMapReduceImpl(sc)
        case SimRankImplType.delta =>
          new NaiveDeltaImpl(sc)
        case SimRankImplType.dfs =>
          new DFSSimRankImpl(sc)
        case SimRankImplType.matrix =>
          new MatrixImpl(sc)
        case SimRankImplType.pagerank =>
          new PageRankLikeMatrixImpl(sc)

        case _ => throw new IOException("unknown simrank impl type " + typ)
      }

      simrankImpl.executeSimRank()
    }
  }
}

object SimRankImplType extends Enumeration {
  val mapreduce, delta, dfs, matrix, pagerank = Value
}
