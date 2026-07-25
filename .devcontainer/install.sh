#!/bin/bash
set -e

echo ">>> 开始安装 JDK 8 和大数据组件（使用本地安装包）<<<"

# 安装 OpenJDK 8
sudo apt-get update
sudo apt-get install -y openjdk-8-jdk
sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

# 写入环境变量
if ! grep -q "JAVA_HOME" ~/.bashrc; then
  cat >> ~/.bashrc << 'ENVEOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export ZOOKEEPER_HOME=/opt/zookeeper
export KAFKA_HOME=/opt/kafka
export SPARK_HOME=/opt/spark
export HBASE_HOME=/opt/hbase
export ES_HOME=/opt/elasticsearch
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$ZOOKEEPER_HOME/bin:$KAFKA_HOME/bin:$SPARK_HOME/bin:$HBASE_HOME/bin:$ES_HOME/bin:$PATH
ENVEOF
  source ~/.bashrc
fi

# 项目根目录下的安装包路径
PROJECT_DIR=/workspaces/five/taobao_11

# Hadoop 2.6.0
if [ ! -d /opt/hadoop ]; then
  sudo tar -xzf $PROJECT_DIR/hadoop-2.6.0.tar.gz -C /opt
  sudo ln -s /opt/hadoop-2.6.0 /opt/hadoop
  sudo chown -R $(whoami) /opt/hadoop-2.6.0 /opt/hadoop
  cat > $HADOOP_CONF_DIR/core-site.xml << EOF
<?xml version="1.0"?>
<configuration>
    <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
EOF
  cat > $HADOOP_CONF_DIR/hdfs-site.xml << EOF
<?xml version="1.0"?>
<configuration>
    <property><name>dfs.replication</name><value>1</value></property>
    <property><name>dfs.namenode.name.dir</name><value>file:///tmp/hadoop/dfs/name</value></property>
    <property><name>dfs.datanode.data.dir</name><value>file:///tmp/hadoop/dfs/data</value></property>
</configuration>
EOF
  hdfs namenode -format
fi
start-dfs.sh

# ZooKeeper 3.4.14
if [ ! -d /opt/zookeeper ]; then
  sudo tar -xzf $PROJECT_DIR/zookeeper-3.4.14.tar.gz -C /opt
  sudo ln -s /opt/zookeeper-3.4.14 /opt/zookeeper
  sudo chown -R $(whoami) /opt/zookeeper-3.4.14 /opt/zookeeper
  cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg
  sed -i 's|dataDir=/tmp/zookeeper|dataDir=/tmp/zookeeper/data|' /opt/zookeeper/conf/zoo.cfg
  mkdir -p /tmp/zookeeper/data
fi
zkServer.sh start

# Kafka 2.0.0
if [ ! -d /opt/kafka ]; then
  sudo tar -xzf $PROJECT_DIR/kafka_2.11-2.0.0.tgz -C /opt
  sudo ln -s /opt/kafka_2.11-2.0.0 /opt/kafka
  sudo chown -R $(whoami) /opt/kafka_2.11-2.0.0 /opt/kafka
  # 配置 zookeeper chroot
  sed -i 's|zookeeper.connect=localhost:2181|zookeeper.connect=localhost:2181/kafka200|' /opt/kafka/config/server.properties
  echo "listeners=PLAINTEXT://0.0.0.0:9092" >> /opt/kafka/config/server.properties
  echo "advertised.listeners=PLAINTEXT://localhost:9092" >> /opt/kafka/config/server.properties
fi
kafka-server-start.sh -daemon /opt/kafka/config/server.properties
sleep 5
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181/kafka200 --replication-factor 1 --partitions 3 --topic orderTopic 2>/dev/null || true
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181/kafka200 --replication-factor 1 --partitions 3 --topic orderEtlTopic 2>/dev/null || true

# Spark 2.4.5（仍然从华为镜像下载，因为项目里没有）
if [ ! -d /opt/spark ]; then
  cd /tmp
  wget -q https://mirrors.huaweicloud.com/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.6.tgz
  sudo tar -xzf spark-2.4.5-bin-hadoop2.6.tgz -C /opt
  sudo ln -s /opt/spark-2.4.5-bin-hadoop2.6 /opt/spark
  sudo chown -R $(whoami) /opt/spark-2.4.5-bin-hadoop2.6 /opt/spark
fi

# HBase 1.2.0
if [ ! -d /opt/hbase ]; then
  sudo tar -xzf $PROJECT_DIR/hbase-1.2.0-bin.tar.gz -C /opt
  sudo ln -s /opt/hbase-1.2.0 /opt/hbase
  sudo chown -R $(whoami) /opt/hbase-1.2.0 /opt/hbase
  cat > /opt/hbase/conf/hbase-site.xml << EOF
<?xml version="1.0"?>
<configuration>
    <property><name>hbase.cluster.distributed</name><value>true</value></property>
    <property><name>hbase.rootdir</name><value>hdfs://localhost:9000/hbase</value></property>
    <property><name>hbase.zookeeper.quorum</name><value>localhost</value></property>
    <property><name>hbase.zookeeper.property.clientPort</name><value>2181</value></property>
    <property><name>zookeeper.znode.parent</name><value>/hbase</value></property>
</configuration>
EOF
  sed -i 's|# export HBASE_MANAGES_ZK=true|export HBASE_MANAGES_ZK=false|' /opt/hbase/conf/hbase-env.sh
fi
start-hbase.sh
sleep 5
echo "create 'htb_orders', 'info'" | hbase shell 2>/dev/null || true

# Elasticsearch 6.0.0（仍然需要从网络下载）
if [ ! -d /opt/elasticsearch ]; then
  cd /tmp
  wget -q https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.0.tar.gz
  sudo tar -xzf elasticsearch-6.0.0.tar.gz -C /opt
  sudo ln -s /opt/elasticsearch-6.0.0 /opt/elasticsearch
  sudo chown -R $(whoami) /opt/elasticsearch-6.0.0 /opt/elasticsearch
  sudo sed -i 's/-Xms[0-9]*[a-z]/-Xms384m/' /opt/elasticsearch/config/jvm.options
  sudo sed -i 's/-Xmx[0-9]*[a-z]/-Xmx384m/' /opt/elasticsearch/config/jvm.options
  echo "discovery.type: single-node" >> /opt/elasticsearch/config/elasticsearch.yml
  echo "bootstrap.memory_lock: false" >> /opt/elasticsearch/config/elasticsearch.yml
  echo "xpack.security.enabled: false" >> /opt/elasticsearch/config/elasticsearch.yml
fi
setsid /opt/elasticsearch/bin/elasticsearch > /tmp/es.log 2>&1 &
sleep 15

# Redis
sudo apt-get install -y redis-server
sudo service redis-server start 2>/dev/null || redis-server --daemonize yes

echo ">>> 环境搭建完成！ <<<"
