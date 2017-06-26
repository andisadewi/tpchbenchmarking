package de.tuberlin.dima.bdapro.flink.tpch.streaming;

import java.time.LocalDate;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka.KafkaConfig;
import de.tuberlin.dima.bdapro.flink.tpch.streaming.queries.Query1;

public class BenchmarkingJob {

	private Properties props;

	public static void main(final String[] args) {
		BenchmarkingJob job = new BenchmarkingJob();
		job.run();	
	}

	private void run() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		setProps();

		DataStream<Tuple16<Integer, Integer, Integer, Integer, Double, 
		Double, Double, Double, String, String, String, 
		String, String, String, String, String>> lineitem = env
				.addSource(new FlinkKafkaConsumer010<>("lineitem", new SimpleStringSchema(), props))
				.rebalance()
				.map(new MapToLineitem());
				
		executeQuery1(lineitem.project(4,5,6,7,8,9,10));
//		tableEnv.registerDataStream("lineitem", lineitem, "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, " +
//				"l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, " +
//				"l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment");
//
//		Query1 query = new Query1(tableEnv, "1.0");
//		query.execute();
		
		
//		tableEnv.sql("select * from nation");
//		TableSink sink = new CsvTableSink("outputNation", "|");
//		sink = sink.configure(new String[] { "name" }, new TypeInformation[] { Types.STRING() });
//		tableEnv.writeToSink(nat, sink);
//
//		Table nat2 = tableEnv.ingest("customer");
//		tableEnv.sql("select * from customer");
//		TableSink sink2 = new CsvTableSink("outputCust", "|");
//		sink2 = sink2.configure(new String[] { "name" }, new TypeInformation[] { Types.STRING() });
//		tableEnv.writeToSink(nat2, sink2);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// TODO

		// long start = System.currentTimeMillis();
		// System.out.println("start: " + start);
		// final Query1 q13 = new Query1(tableEnv, sf);
		// q13.execute();
		// long end = System.currentTimeMillis();
		// System.out.println("end: " + end);
		// System.out.println("diff: " + (end-start));
		
	}

	private void executeQuery1(
			final DataStream<Tuple7<Double, Double, Double, Double, String, String, String>> lineitem) {
		lineitem
		.filter(new FilterFunction<Tuple7<Double, Double, Double, Double, String, String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(final Tuple7<Double, Double, Double, Double, String, String, String> arg0)
					throws Exception {
				final LocalDate thresholdDate = LocalDate.of(1998, 12, 1).minusDays(90);
				LocalDate date = LocalDate.parse(arg0.f6);
				return date.isBefore(thresholdDate) || date.isEqual(thresholdDate);
			}
		}).keyBy(4,5).reduce(new ReduceFunction<Tuple7<Double,Double,Double,Double,String,String,String>>() {
			
			@Override
			public Tuple7<Double, Double, Double, Double, String, String, String> reduce(
					Tuple7<Double, Double, Double, Double, String, String, String> value1,
					Tuple7<Double, Double, Double, Double, String, String, String> value2) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
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

	private class MapToLineitem implements
			MapFunction<String, Tuple16<Integer, Integer, Integer, Integer, Double, Double, Double, Double, String, String, String, String, String, String, String, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple16<Integer, Integer, Integer, Integer, Double, Double, Double, Double, String, String, String, String, String, String, String, String> map(
				final String value) throws Exception {
			String[] strArr = value.split("|");
			assert strArr.length == 16;
						
			return new Tuple16<Integer, Integer, Integer, Integer, Double, Double, Double, Double, String, String, String, String, String, String, String, String>
			(Integer.valueOf(strArr[0]), Integer.valueOf(strArr[1]), Integer.valueOf(strArr[2]), Integer.valueOf(strArr[3]), 
					Double.valueOf(strArr[4]), Double.valueOf(strArr[5]), Double.valueOf(strArr[6]), Double.valueOf(strArr[7]), strArr[8], strArr[9], strArr[10], 
					strArr[11], strArr[12], strArr[13], strArr[14], strArr[15]);
		}

	}

}
