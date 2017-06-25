cd kafka_2.11-0.10.0.0

# stop broker
sudo ./bin/kafka-server-stop.sh

sleep 3
echo "Kafka server is stopped."

# stop zookeeper server
sudo ./bin/zookeeper-server-stop.sh

echo "Zookeeper server is stopped."
