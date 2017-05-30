package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

public class Query1 extends Query {

	public Query1(final BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>> execute() {
		return execute(Utils.getRandomInt(60, 120));
	}

	public List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>> execute(
			final int delta) {
		Table lineitem = env.scan("lineitem");

		Table result = lineitem.where("shipdate.toDate <= ('1998-12-01'.toDate - " + delta + ".days)")
				.groupBy("returnflag, linestatus")
				.select("returnflag, linestatus, sum(quantity) as sum_qty, "
						+ "sum(extendedprice) as sum_base_price, "
						+ "sum(extendedprice*(1-discount)) as sum_disc_price, "
						+ "sum(extendedprice*(1-discount)*(1+tax)) as sum_charge, "
						+ "avg(quantity) as avg_qty, "
						+ "avg(extendedprice) as avg_price, "
						+ "avg(discount) as avg_disc, "
						+ "count(linestatus) as count_order")
				.orderBy("returnflag, linestatus");

		try {
			return env.toDataSet(result, TypeInformation.of
					(new TypeHint<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>>(){}))
					.map(new MapFunction<Tuple10<String,String,Double,Double,Double,Double,Double,Double,Double,Long>, Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> map(
								final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> value)
										throws Exception {
							return Utils.keepOnlyTwoDecimals(value);
						}
					}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
