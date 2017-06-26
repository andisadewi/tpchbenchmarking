package de.tuberlin.dima.bdapro.flink.tpch.streaming;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

import de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka.KafkaConfig;

public class BenchmarkingJob {

	private static Properties props;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(final String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		setProps();

		DataStream<String> nation = env
				.addSource(new FlinkKafkaConsumer010<>("nation", new SimpleStringSchema(), props))
				.map(new MapFunction<String, String>() {
					private static final long serialVersionUID = -6867736771747690202L;

					@Override
					public String map(final String value) throws Exception {
						return "Stream Value: " + value;
					}
				});

		DataStream<String> customer = env
				.addSource(new FlinkKafkaConsumer010<>("customer", new SimpleStringSchema(), props))
				.map(new MapFunction<String, String>() {
					private static final long serialVersionUID = -6867736771747690202L;

					@Override
					public String map(final String value) throws Exception {
						return "Stream Value: " + value;
					}
				});

		tableEnv.registerDataStream("nation", nation, "name");
		tableEnv.registerDataStream("customer", customer, "name");
		
		Table nat = tableEnv.ingest("nation");
		tableEnv.sql("select * from nation");
		TableSink sink = new CsvTableSink("outputNation", "|");
		sink = sink.configure(new String[]{"name"}, new TypeInformation[] {Types.STRING()});
		tableEnv.writeToSink(nat, sink);
		
		Table nat2 = tableEnv.ingest("customer");
		tableEnv.sql("select * from customer");
		TableSink sink2 = new CsvTableSink("outputCust", "|");
		sink2 = sink2.configure(new String[]{"name"}, new TypeInformation[] {Types.STRING()});
		tableEnv.writeToSink(nat2, sink2);
		
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

		//TODO 
		
		// long start = System.currentTimeMillis();
		// System.out.println("start: " + start);
		// final Query1 q13 = new Query1(tableEnv, sf);
		// q13.execute();
		// long end = System.currentTimeMillis();
		// System.out.println("end: " + end);
		// System.out.println("diff: " + (end-start));
	}

	private static void setProps() {
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
