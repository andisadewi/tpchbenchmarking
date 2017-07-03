# please start kafka server before executing this script

cd kafka_2.11-0.10.0.0

# create topic
./bin/kafka-topics.sh --create --topic nation --zookeeper localhost:2181 --partitions 1 --replication-factor 1

./bin/kafka-topics.sh --create --topic consumer --zookeeper localhost:2181 --partitions 1 --replication-factor 1

./bin/kafka-topics.sh --create --topic lineitem --zookeeper localhost:2181 --partitions 1 --replication-factor 1
