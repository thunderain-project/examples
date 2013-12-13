package org.apache.spark.rdd

import org.apache.spark._

class SplitsRDD[K: ClassManifest, V: ClassManifest](
  val rdd: RDD[(K, V)],
  partitioner: Partitioner) extends RDD[(K, V)](rdd.context, Nil){

  override def getPartitions: Array[Partition] = rdd.partitions.flatMap { p =>
    (0 until partitioner.numPartitions).map { i =>
      new OnePartition(rdd, p.index * partitioner.numPartitions + i, p.index)
    }
  }.toArray

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[OnePartition]
    rdd.preferredLocations(currSplit.s)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val part = split.asInstanceOf[OnePartition]
    val idx = part.index % partitioner.numPartitions
    rdd.iterator(part.s, context).filter(t => partitioner.getPartition(t._1) == idx)
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd) {
      def getParents(partitionId: Int): Seq[Int] = List(partitionId / partitioner.numPartitions)
    }
  )
}

object SplitsRDD {
  def splits[K: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)], part: Partitioner) = {
    new SplitsRDD(rdd, part)
  }
}
