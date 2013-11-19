
package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.{Dependency, NarrowDependency, Partition, SparkContext,
  SparkEnv, TaskContext}
import org.apache.spark.storage.{StorageLevel, RDDBlockId}

class CartesianPartitionsPartition(
  idx: Int,
  @transient rdd1: RDD[_],
  @transient rdd2: RDD[_],
  s1Index: Int,
  s2Index: Int) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

class CartesianPartitionsRDD[T: ClassManifest, U: ClassManifest, V: ClassManifest](
  sc: SparkContext,
  f: (Iterator[T], Iterator[U], Int, Int) => Iterator[V],
  var rdd1: RDD[T],
  var rdd2: RDD[U]) extends RDD[V](sc, Nil) {

  val numPartitionsRdd2 = rdd2.partitions.size

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdd1.partitions.size * rdd2.partitions.size)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsRdd2 + s2.index
      array(idx) = new CartesianPartitionsPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartitionsPartition]
    rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)
  }

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partition = s.asInstanceOf[CartesianPartitionsPartition]

    val env = SparkEnv.get
    val blockManager = env.blockManager
    val key = RDDBlockId(rdd2.id, partition.s2.index)
    blockManager.getLocal(key) match {
      case None =>
        blockManager.getRemote(key).foreach { iter =>
          blockManager.put(key, iter, StorageLevel.MEMORY_AND_DISK_SER, true)
        }
      case Some(iter) =>
        println(">>>>>get local key:" + key.name)
        Unit
    }

    println(">>>>>compute part1: " + partition.s1.index + ", part2: " + partition.s2.index)
    f(rdd1.iterator(partition.s1, context),
      rdd2.iterator(partition.s2, context),
      partition.s1.index,
      partition.s2.index)
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(partitionId: Int): Seq[Int] = List(partitionId / numPartitionsRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(partitionId: Int): Seq[Int] = List(partitionId % numPartitionsRdd2)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}

object CartesianPartitionsRDD {
  def cartesianPartitions[T: ClassManifest, U: ClassManifest, V: ClassManifest]
    (rdd1: RDD[T], rdd2: RDD[U], sc: SparkContext)
    (f: (Iterator[T], Iterator[U], Int, Int) => Iterator[V]): RDD[V] = {
    new CartesianPartitionsRDD(sc, sc.clean(f), rdd1, rdd2)
  }
}


