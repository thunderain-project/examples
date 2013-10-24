package simrank

import java.io.{BufferedReader, FileReader, IOException}

import scala.collection.mutable

trait AbstractSimRankImpl extends Serializable {

  val regex = "\\((\\d+), (\\d+)\\) (\\d+\\.\\d+)".r

  val graphPath = getVerifiedProperty("simrank.graph.data")
  val graphSize = getVerifiedProperty("simrank.graph.size").toInt

  val iterations = getVerifiedProperty("simrank.iterations").toInt

  def getVerifiedProperty(key: String): String = {
    Option(SimRankImpl.getProp.getProperty(key)).getOrElse {
      throw new IOException(key + " should be set")
    }
  }

  def initializeGraphData(data: String): Array[((Long, Long), Double)] = {
    var is: BufferedReader = null
    try {
      is = new BufferedReader(new FileReader(data))

      val graph = new mutable.ArrayBuffer[((Long, Long), Double)]
      var line = is.readLine()
      while (line != null) {
        val regex(row, col, value) = line
        graph.append(((row.toLong, col.toLong), value.toDouble))

        line = is.readLine()
      }

      graph.toArray
    } catch {
      case e: Exception => throw new IOException("failed to read graph data")
    }
  }

  def executeSimRank(): Unit}
