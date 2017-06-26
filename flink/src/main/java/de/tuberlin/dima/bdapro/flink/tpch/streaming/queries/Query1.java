package de.tuberlin.dima.bdapro.flink.tpch.streaming.queries;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

public class Query1 extends Query {

	public Query1(final StreamTableEnvironment env, final String sf) {
		super(env, sf);
	}

	@Override
	public void execute() {
		execute(Utils.getRandomInt(60, 120));
	}

	public void execute(final int delta) {
		Table lineitem = env.ingest("lineitem");

		Table result = lineitem
				.where("l_shipdate.toDate <= ('1998-12-01'.toDate - " + delta + ".days)")
				.window(Tumble.over("100000.rows").as("w"))				
				.groupBy("w, l_returnflag, l_linestatus")				
				.select("l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, "
						+ "sum(l_extendedprice) as sum_base_price, "
						+ "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, "
						+ "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, "
						+ "avg(l_quantity) as avg_qty, "
						+ "avg(l_extendedprice) as avg_price, "
						+ "avg(l_discount) as avg_disc, "
						+ "count(l_linestatus) as count_order")				
				.orderBy("l_returnflag, l_linestatus");

		try {
			env.toDataStream(result, TypeInformation.of
					(new TypeHint<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>>(){}))
			.map(new MapFunction<Tuple10<String,String,Double,Double,Double,Double,Double,Double,Double,Long>, Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> map(
						final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> value)
								throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).writeAsText("query1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
