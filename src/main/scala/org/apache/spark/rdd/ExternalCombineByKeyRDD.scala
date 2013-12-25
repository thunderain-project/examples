package org.apache.spark.rdd

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

import java.io.FileInputStream

import org.apache.spark._
import org.apache.spark.storage.TaskResultBlockId

class ExternalCombineByKeyRDD[
  K: ClassManifest,
  V: ClassManifest,
  P: ClassManifest](
  val rdd: RDD[(K, P)],
  val aggregator: Aggregator[K, V, P],
  partitioner: Partitioner) extends RDD[(K, P)](rdd){

  override def getPartitions: Array[Partition] = firstParent[(K, P)].partitions

  val partitionSize = firstParent[(K, P)].partitions.size

  override def compute(split: Partition, context: TaskContext): Iterator[(K, P)] = {
    val env = SparkEnv.get
    val blockManager = env.blockManager
    val ser = env.serializer
    val keys = Array.tabulate(partitioner.numPartitions) { i =>
      // avoid block id duplication
      TaskResultBlockId(rdd.id * partitionSize * partitioner.numPartitions +
        split.index * partitioner.numPartitions + i)
    }
    val files = keys.map(blockManager.diskBlockManager.getFile(_))
    val diskWriters = keys.zip(files).map {
      r => blockManager.getDiskWriter(r._1, r._2, ser, 128 * 1024)
    }

    firstParent[(K, P)].iterator(split, context).foreach { record =>
      diskWriters(partitioner.getPartition(record._1)).write(record)
    }
    diskWriters.foreach { w =>
      w.commit()
      w.close()
    }

    files.filter(_.exists()).toIterator.flatMap { f =>
      val fis = new FileInputStream(f)
      val bis = new FastBufferedInputStream(fis, 128 * 1024)
      val iter = ser.newInstance().deserializeStream(bis)
        .asIterator
        .asInstanceOf[Iterator[(K, P)]]

      val ret = new InterruptibleIterator(context, aggregator.combineCombinersByKey(iter))
      bis.close()
      f.delete()

      ret.asInstanceOf[Iterator[(K, P)]]
    }
  }
}

object ExternalCombineByKeyRDD {
  def combineByKey[K: ClassManifest, V: ClassManifest, P: ClassManifest](
    rdd: RDD[(K, P)], part: Partitioner, aggregator: Aggregator[K, V, P]) = {
    new ExternalCombineByKeyRDD(rdd, aggregator, part).asInstanceOf[RDD[(K, P)]]
  }
}

