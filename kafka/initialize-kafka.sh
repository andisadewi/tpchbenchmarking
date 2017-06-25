#get kafka
wget http://mirror.softaculous.com/apache//kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz

#unpack
tar xf kafka_2.11-0.10.0.0.tgz
cd kafka_2.11-0.10.0.0

# start zookeeper server
./bin/zookeeper-server-start.sh ./config/zookeeper.properties > /dev/null &

sleep 5
echo "Zookeeper server is started..."

# start broker
./bin/kafka-server-start.sh ./config/server.properties > /dev/null &

sleep 5
echo "Kafka server is started..."

rm kafka_2.11-0.10.0.0.tgz
