#!/usr/bin/env bash

#export SPARK_HOME=/opt/spark_20130729

#export SCALA_HOME=/opt/scala-2.9.3

#export SPARK_MEM=15g

#export JAVA_HOME=/usr/java/jdk1.6.0_30

#export MESOS_NATIVE_LIBRARY=/opt/mesos-dist-trunk/lib/libmesos.so
#export MESOS_NATIVE_LIBRARY=/opt/mesos-0.12.0-dist/lib/libmesos.so

SPARK_JAVA_OPTS="-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps -Dspark.speculation=true "

export SPARK_JAVA_OPTS
