#!/bin/bash

# 네임스페이스 디렉토리를 입력받아서 
NAME_DIR=$1
echo "Namenode DIR : $NAME_DIR"

# 비어있지 않다면 이미 포맷된 것이므로
if [ "$(ls -A $NAME_DIR | grep -v 'leveldb-timeline-store.ldb')" ]; then
  echo "already formatted"
else
 echo "Y" | $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR  namenode -format
fi

# NameNode 기동
$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode
