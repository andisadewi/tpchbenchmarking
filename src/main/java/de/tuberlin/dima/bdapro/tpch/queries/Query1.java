package de.tuberlin.dima.bdapro.tpch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

import de.tuberlin.dima.bdapro.tpch.Config;

public class Query1 extends Query {

	public Query1(final ExecutionEnvironment env, final String sf) {
		super(env, sf);
	}

	@Override
	public List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> execute() {
		return execute(getRandomInt(60, 120));
	}

	public List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> execute(
			final int delta) {

		final LocalDate thresholdDate = LocalDate.of(1998, 12, 1).minusDays(delta);

		// quantity, extprice, discount, tax, return flag, linestatus, shipdate
		final DataSet<Tuple7<Double, Double, Double, Double, String, String, String>> lineitems = readLineitem();

		try {
			return lineitems
					.filter(new FilterFunction<Tuple7<Double, Double, Double, Double, String, String, String>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public boolean filter(final Tuple7<Double, Double, Double, Double, String, String, String> arg0)
								throws Exception {
							LocalDate date = LocalDate.parse(arg0.f6);
							return date.isBefore(thresholdDate) || date.isEqual(thresholdDate);
						}
					}).groupBy(4, 5).reduceGroup(
							new GroupReduceFunction<Tuple7<Double, Double, Double, Double, String, String, String>, Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>>() {
								private static final long serialVersionUID = 1L;

								@Override
								public void reduce(
										final Iterable<Tuple7<Double, Double, Double, Double, String, String, String>> values,
										final Collector<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> out)
												throws Exception {
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
									for (Tuple7<Double, Double, Double, Double, String, String, String> tuple : values) {
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

									out.collect(keepOnlyTwoDecimals(
											new Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>(
													returnFlag, linestatus, sumQuantity, sumExtendedPrice,
													sumExtPriceDiscount, sumExtPriceDiscountTax, avgQuantity,
													avgExtendedPrice, avgDiscount, count)));

								}
							})
					.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.ASCENDING).collect();
		} catch (final Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private DataSet<Tuple7<Double, Double, Double, Double, String, String, String>> readLineitem() {
		final CsvReader source = getCSVReader(Config.LINEITEM);

		return source.fieldDelimiter("|").includeFields("0000111111100000").types(Double.class, Double.class,
				Double.class, Double.class, String.class, String.class, String.class);
	}
}
