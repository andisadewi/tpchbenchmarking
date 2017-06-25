package de.tuberlin.dima.bdapro.flink.tpch.streaming;

import de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka.KafkaConsumer;

public class BenchmarkingJob {

	public static void main(final String[] args) {

		KafkaConsumer consumer = new KafkaConsumer();
		consumer.startReceiving();

		// String sf = "1.0";
		// StreamExecutionEnvironment env =
		// StreamExecutionEnvironment.getExecutionEnvironment();
		// TableSourceProvider provider = new TableSourceProvider();
		// provider.setBaseDir("");
		// StreamTableEnvironment tableEnv = provider.loadDataStream(env, sf);
		//
		// long start = System.currentTimeMillis();
		// System.out.println("start: " + start);
		// final Query1 q13 = new Query1(tableEnv, sf);
		// q13.execute();
		// long end = System.currentTimeMillis();
		// System.out.println("end: " + end);
		// System.out.println("diff: " + (end-start));
	}

}
