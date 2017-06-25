cd kafka_2.11-0.10.0.0

# start zookeeper server
sudo ./bin/zookeeper-server-start.sh ./config/zookeeper.properties > /dev/null &

echo "Zookeeper server started"

sleep 3

# start broker
sudo ./bin/kafka-server-start.sh ./config/server.properties > /dev/null & 

echo "Kafka server started"
