#!/usr/bin/env bash

#export SPARK_HOME=/opt/spark_20130729

export SCALA_HOME=/home/jerryshao/bin/scala-2.9.3

#export SPARK_MEM=15g

export JAVA_HOME=/usr/lib/java/jdk1.6.0_38

#SPARK_JAVA_OPTS+="-Dspark.serializer=org.apache.spark.serializer.KryoSerializer "
#SPARK_JAVA_OPTS+="-Dspark.shuffle.compress=false "
#SPARK_JAVA_OPTS+="-Dspark.shuffle.use.netty=true "
#SPARK_JAVA_OPTS+="-Dspark.shuffle.copier.threads=8 "
#SPARK_JAVA_OPTS+="-Dspark.shuffle.consolidateFiles=true "
#SPARK_JAVA_OPTS+="-Dspark.shuffle.file.buffer.kb=192 "
#SPARK_JAVA_OPTS+="-Dspark.kryo.registrator=simrank.MatrixElementKryoSerializer "
#SPARK_JAVA_OPTS+="-Dspark.local.dir=/mnt/DP_disk6/spark,/mnt/DP_disk3/spark,/mnt/DP_disk4/spark,/mnt/DP_disk5/spark,/mnt/DP_disk7/spark,/mnt/DP_disk8/spark,/mnt/DP_disk9/spark,/mnt/DP_disk10/spark,/mnt/DP_disk11/spark,/mnt/DP_disk12/spark"
SPARK_JAVA_OPTS+="-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeRefBLAS "

export SPARK_JAVA_OPTS
