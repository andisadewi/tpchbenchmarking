package de.tuberlin.dima.bdapro.flink.tpch.streaming;

import java.time.LocalDate;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.streaming.kafka.KafkaConfig;

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

//        FlinkKafkaConsumer010<String> lineitem2 = (FlinkKafkaConsumer010<String>) new FlinkKafkaConsumer010<>("lineitem", new SimpleStringSchema(), props)
//             .assignTimestampsAndWatermarks(new IngestionTimeExtractor());
        
        // TODO calculate throughput, look at the yahoo example 
        
        FlinkKafkaConsumer010<String> lineitem2 = (FlinkKafkaConsumer010<String>) new FlinkKafkaConsumer010<>("lineitem", new SimpleStringSchema(), props)
        		// assign current time as timestamp (this is time when the data is received) 
        		.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public long extractAscendingTimestamp(final String element) {
						return System.currentTimeMillis();
					}
        		});
        
        DataStream<Tuple16<Integer, Integer, Integer, Integer, Double,
                Double, Double, Double, String, String, String,
                String, String, String, String, String>> lineitem = env
                .addSource(lineitem2)
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

    private FlinkKafkaConsumer010<String> getLineitem() {
        return new FlinkKafkaConsumer010<>("lineitem", new SimpleStringSchema(), props);
    }

    private void executeQuery1(
			final DataStream<Tuple7<Double, Double, Double, Double, String, String, String>> lineitem) {
        SingleOutputStreamOperator<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> pricingSummary = lineitem
                .filter(new FilterFunction<Tuple7<Double, Double, Double, Double, String, String, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(final Tuple7<Double, Double, Double, Double, String, String, String> arg0)
                            throws Exception {
                        final LocalDate thresholdDate = LocalDate.of(1998, 12, 1).minusDays(90);
                        LocalDate date = LocalDate.parse(arg0.f6);
                        return date.isBefore(thresholdDate) || date.isEqual(thresholdDate);
                    }
                }).keyBy(4, 5)
                .timeWindow(Time.seconds(10), Time.seconds(1))
                .apply(new PricingSummaryReport());

        		// TODO after executing the query, calculate latency 
        		// i guess: current time - timestamp of FIRST tuple in a window - window time
    }


//    public static class orderPricingReportLineStatus implements ProcessFunction<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>,
//                    Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> {
//        private ValueState<PriorityQueue<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>>> queueState = null;
//        @Override
//        public void open(Configuration config) {
//
//            ValueStateDescriptor<PriorityQueue<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>>> descriptor
//                    = new ValueStateDescriptor<>(
//                    // state name
//                    "sorted-events",
//                    // type information of state
//                    TypeInformation.of(new TypeHint<PriorityQueue<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>>>() {
//                    }));
//            queueState = getRuntimeContext().getState(descriptor);
//        }
//
//        @Override
//        public void processElement(Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> event,
//                                   Context context, Collector<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> out) throws Exception {
//            TimerService timerService = context.timerService();
//
//            if (context.timestamp() > timerService.currentWatermark()) {
//                PriorityQueue<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> queue = queueState.value();
//                if (queue == null) {
//                    queue = new PriorityQueue<>(10, new orderByFunction());
//                }
//                queue.add(event);
//                queueState.update(queue);
//                timerService.registerEventTimeTimer(event.timestamp);
//            }
//        }
//
//        @Override
//        public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> out) throws Exception {
//            PriorityQueue<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> queue = queueState.value();
//            Long watermark = context.timerService().currentWatermark();
//            Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> head = queue.peek();
//            while (head != null && head.timestamp <= watermark) {
//                out.collect(head);
//                queue.remove(head);
//                head = queue.peek();
//            }
//        }
//
//
//    }

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
			String[] strArr = value.split("\\|");
			assert strArr.length == 16;
						
			return new Tuple16<Integer, Integer, Integer, Integer, Double, Double, Double, Double, String, String, String, String, String, String, String, String>
			(Integer.valueOf(strArr[0]), Integer.valueOf(strArr[1]), Integer.valueOf(strArr[2]), Integer.valueOf(strArr[3]), 
					Double.valueOf(strArr[4]), Double.valueOf(strArr[5]), Double.valueOf(strArr[6]), Double.valueOf(strArr[7]), strArr[8], strArr[9], strArr[10], 
					strArr[11], strArr[12], strArr[13], strArr[14], strArr[15]);
		}

	}

    private static class PricingSummaryReport implements WindowFunction<Tuple7<Double, Double, Double, Double, String, String, String>,
                Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>, Tuple, TimeWindow>
    {
        @Override
        public void apply(final Tuple aLong, final TimeWindow window,
                          final Iterable<Tuple7<Double, Double, Double, Double, String, String, String>> input,
                          final Collector<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> out) {
            double sumQuantity = 0;
            double sumExtendedPrice = 0;
            double sumExtPriceDiscount = 0;
            double sumExtPriceDiscountTax = 0;
            double avgQuantity = 0;
            double avgExtendedPrice = 0;
            double avgDiscount = 0;
            int count = 0;
            String returnFlag = "";
            String linestatus = "";
            for (Tuple7<Double, Double, Double, Double, String, String, String> tuple : input) {
                sumQuantity += tuple.f0;
                sumExtendedPrice += tuple.f1;
                sumExtPriceDiscount += tuple.f1 * (1 - tuple.f2);
                sumExtPriceDiscountTax += tuple.f1 * (1 - tuple.f2) * (1 + tuple.f3);
                avgDiscount += tuple.f2;
                count++;
                returnFlag = tuple.f4;
                linestatus = tuple.f5;
            }
            avgQuantity = sumQuantity / count;
            avgExtendedPrice = sumExtendedPrice / count;
            avgDiscount /= count;

            out.collect(Utils.keepOnlyTwoDecimals(
                    new Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>(
                            returnFlag, linestatus, sumQuantity, sumExtendedPrice,
                            sumExtPriceDiscount, sumExtPriceDiscountTax, avgQuantity,
                            avgExtendedPrice, avgDiscount, count)));
        }
    }
}

