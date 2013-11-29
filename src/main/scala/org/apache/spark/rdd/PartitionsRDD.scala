package org.apache.spark.rdd

import java.io.{ObjectOutputStream, IOException}

import org.apache.spark._

class OnePartition(@transient rdd: RDD[_], idx: Int, parentIdx: Int) extends Partition {
  override val index: Int = idx
  var s = rdd.partitions(parentIdx)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    s = rdd.partitions(parentIdx)
    oos.defaultWriteObject()
  }
}

class PartitionsRDD[T: ClassManifest](
  val rdd: RDD[T],
  partitions: Array[Partition]) extends RDD[T](rdd){

  override def getPartitions: Array[Partition] = partitions.zipWithIndex.map { kv =>
    new OnePartition(rdd, kv._2, kv._1.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val part = split.asInstanceOf[OnePartition]
    rdd.iterator(part.s, context)
  }
}
