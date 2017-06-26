package de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class KafkaConsumer {

	private Properties props;

	public KafkaConsumer() {
		setProps();
	}

	public void startReceiving() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer010<>("nation", new SimpleStringSchema(), props));

		stream.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public String map(final String value) throws Exception {
				return "Stream Value: " + value;
			}
		}).writeAsText("output.txt");

		DataStream<String> stream2 = env
				.addSource(new FlinkKafkaConsumer010<>("customer", new SimpleStringSchema(), props));

		stream2.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public String map(final String value) throws Exception {
				return "Stream Value: " + value;
			}
		}).writeAsText("output2.txt");

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setProps() {
		props = new Properties();
		props.put(KafkaConfig.TOPIC_NAME, KafkaConfig.TOPIC_NAME_VALUE);
		props.put(KafkaConfig.BOOTSTRAP_SERVER, KafkaConfig.BOOTSTRAP_SERVER_VALUE);
		props.put(KafkaConfig.ZOOKEEPER, KafkaConfig.ZOOKEPER_VALUE);
		props.put(KafkaConfig.GROUP_ID, KafkaConfig.GROUP_ID_VALUE);
		// props.put(KafkaConfig.KEY_DESERIALIZER,
		// KafkaConfig.KEY_DESERIALIZER_VALUE);
		// props.put(KafkaConfig.VALUE_DESERIALIZER,
		// KafkaConfig.VALUE_DESERIALIZER_VALUE);
	}

}
