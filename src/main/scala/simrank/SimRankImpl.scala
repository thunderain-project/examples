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

        case _ => throw new IOException("unknown simrank impl type " + typ)
      }

      simrankImpl.executeSimRank()
    }
  }
}

object SimRankImplType extends Enumeration {
  val mapreduce, delta, dfs, matrix = Value
}
