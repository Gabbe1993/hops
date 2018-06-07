#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/default-java/jre
export HADOOP_HOME=/home/gabriel/Documents/hadoop-prototype/hadoop-dist/target/hadoop-2.8.0-SNAPSHOT
export HADOOP_CONF_DIR=$(pwd)/conf
export HADOOP_LOG_DIR=$(pwd)/log
PATH="$HADOOP_HOME"/bin:$PATH
export HADOOP_CLASSPATH="$HADOOP_HOME"/share/hadoop/tools/lib/*
export HADOOP_USER_CLASSPATH_FIRST=true
fs2img () {
    # e.g. fs2img -b org.apache.hadoop.hdfs.server.common.TextFileRegionFormat s3a://provided-test/
    CP=$(hadoop classpath):"$HADOOP_HOME"/share/hadoop/tools/lib/*
    java -cp "$CP" org.apache.hadoop.hdfs.server.namenode.FileSystemImage -conf "$tmpconf" "$@"
}
