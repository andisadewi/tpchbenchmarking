package de.tuberlin.dima.bdapro.tpch.queries;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

import de.tuberlin.dima.bdapro.tpch.Config;

public class Query1 extends Query {

	private int delta = getDelta();

	public Query1(final ExecutionEnvironment env, final String sf) {
		super(env, sf);
	}

	@Override
	public List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> execute() {

		final LocalDate thresholdDate = LocalDate.of(1998, 12, 1).minusDays(delta);
		final DataSet<Tuple7<Integer, Double, Double, Double, String, String, String>> lineitems = readLineitem();

		try {
			final List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> out = lineitems
					.map(new MapFunction<Tuple7<Integer, Double, Double, Double, String, String, String>, Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple7<Integer, Double, Double, Double, String, String, LocalDate> map(
								final Tuple7<Integer, Double, Double, Double, String, String, String> value)
								throws Exception {
							final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
							return new Tuple7<Integer, Double, Double, Double, String, String, LocalDate>(value.f0,
									value.f1, value.f2, value.f3, value.f4, value.f5,
									LocalDate.parse(value.f6, formatter));
						}
					}).filter(new FilterFunction<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public boolean filter(
								final Tuple7<Integer, Double, Double, Double, String, String, LocalDate> arg0)
								throws Exception {
							return arg0.f6.isBefore(thresholdDate) || arg0.f6.isEqual(thresholdDate);
						}
					}).reduceGroup(
							new GroupReduceFunction<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>, Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>>() {
								private static final long serialVersionUID = 1L;

								@Override
								public void reduce(
										final Iterable<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>> arg0,
										final Collector<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> arg1)
										throws Exception {

									final HashMap<String, List<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>>> orderBy = new HashMap<String, List<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>>>();

									// group by
									loop: for (final Tuple7<Integer, Double, Double, Double, String, String, LocalDate> tuple : arg0) {
										if (orderBy.containsKey(tuple.f4)) {
											final List<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>> list = orderBy
													.get(tuple.f4);
											for (final HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>> elem : list) {
												if (elem.containsKey(tuple.f5)) {
													elem.get(tuple.f5).add(tuple);
													continue loop;
												}
											}
											final List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>> newList = new ArrayList<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>();
											newList.add(tuple);
											final HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>> newHashMap = new HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>();
											newHashMap.put(tuple.f5, newList);
											list.add(newHashMap);
										} else {
											final List<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>> newList = new ArrayList<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>>();
											final HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>> newHashMap = new HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>();
											final List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>> newList2 = new ArrayList<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>();
											newList2.add(tuple);
											newHashMap.put(tuple.f5, newList2);
											newList.add(newHashMap);
											orderBy.put(tuple.f4, newList);
										}
									}

									// calculate sum, avg, count
									final Set<Entry<String, List<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>>>> set = orderBy
											.entrySet();
									for (final Entry<String, List<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>>> returnFlag : set) {
										final List<HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>> list1 = returnFlag
												.getValue();
										for (final HashMap<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>> val2 : list1) {
											final Set<Entry<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>>> set2 = val2
													.entrySet();
											for (final Entry<String, List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>> lineStatus : set2) {
												double sumQuantity = 0;
												double sumExtendedPrice = 0;
												double sumExtPriceDiscount = 0;
												double sumExtPriceDiscountTax = 0;
												double avgQuantity = 0;
												double avgExtendedPrice = 0;
												double avgDiscount = 0;

												final List<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>> list2 = lineStatus
														.getValue();
												final int count = list2.size();
												for (final Tuple7<Integer, Double, Double, Double, String, String, LocalDate> tup : list2) {
													sumQuantity += tup.f0;
													sumExtendedPrice += tup.f1;
													sumExtPriceDiscount += tup.f1 * (1 - tup.f2);
													sumExtPriceDiscountTax += tup.f1 * (1 - tup.f2) * (1 + tup.f3);
													avgDiscount += tup.f2;
												}
												avgQuantity = sumQuantity / count;
												avgExtendedPrice = sumExtendedPrice / count;
												avgDiscount /= count;

												arg1.collect(keepOnlyTwoDecimals(
														new Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>(
																returnFlag.getKey(), lineStatus.getKey(), sumQuantity,
																sumExtendedPrice, sumExtPriceDiscount,
																sumExtPriceDiscountTax, avgQuantity, avgExtendedPrice,
																avgDiscount, count)));
											}
										}
									}
								}
							})
					.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.ASCENDING).collect();
			return out;
		} catch (final Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private DataSet<Tuple7<Integer, Double, Double, Double, String, String, String>> readLineitem() {
		final CsvReader source = getCSVReader(Config.LINEITEM);
		return source.fieldDelimiter("|").includeFields("0000111111100000").types(Integer.class, Double.class,
				Double.class, Double.class, String.class, String.class, String.class);
	}

	private int getDelta() {
		final Random rand = new Random();
		return 60 + rand.nextInt((120 - 60) + 1);
	}

	public void setDelta(final int delta) {
		this.delta = delta;
	}

}
