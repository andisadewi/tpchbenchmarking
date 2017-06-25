package de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka;

import java.io.File;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;

public class KafkaProducer {

	private Properties props;

	private String path;

	public static void main(final String[] args) {
		//////////////////////// ARGUMENT PARSING
		//////////////////////// ///////////////////////////////////////////////////////
		if (args.length <= 0 || args.length > 2) {
			throw new IllegalArgumentException(
					"Please input the path to the directory where the test databases are located.");
		}
		String path = args[0];
		try {
			File file = new File(path);
			if (!(file.isDirectory() && file.exists())) {
				throw new IllegalArgumentException(
						"Please give a valid path to the directory where the test databases are located.");
			}
		} catch (Exception ex) {
			throw new IllegalArgumentException(
					"Please give a valid path to the directory where the test databases are located.");
		}

		String sf = args[1];
		try {
			Double.parseDouble(sf);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Please give a valid scale factor.");
		}

		KafkaProducer producer = new KafkaProducer();
		producer.path = path + sf + "/";
		producer.startSending();

	}

	public KafkaProducer() {
		setProps();
	}

	public void startSending() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stream = env.readTextFile(path + PathConfig.NATION);
		stream.addSink(new FlinkKafkaProducer010<>("nation", new SimpleStringSchema(), props));

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setProps() {
		props = new Properties();
		props.put(KafkaConfig.BOOTSTRAP_SERVER, KafkaConfig.BOOTSTRAP_SERVER_VALUE);
		props.put(KafkaConfig.ACK, KafkaConfig.ACK_VALUE);
		props.put(KafkaConfig.RETRIES, KafkaConfig.RETRIES_VALUE);
		props.put(KafkaConfig.BATCH_SIZE, KafkaConfig.BATCH_SIZE_VALUE);
		props.put(KafkaConfig.LINGER, KafkaConfig.LINGER_VALUE);
		props.put(KafkaConfig.BUFFER, KafkaConfig.BUFFER_VALUE);
		// props.put(KafkaConfig.KEY_SERIALIZER,
		// KafkaConfig.KEY_SERIALIZER_VALUE);
		// props.put(KafkaConfig.VALUE_SERIALIZER,
		// KafkaConfig.VALUE_SERIALIZER_VALUE);
	}

}
