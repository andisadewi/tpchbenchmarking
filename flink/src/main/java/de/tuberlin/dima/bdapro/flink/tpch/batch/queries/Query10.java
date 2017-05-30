package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;

public class Query10 extends Query {

	public Query10(final BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple8<Integer, String, Double, Double, String, String, String, String>> execute() {
		return execute(getRandomDate());
	}

	public List<Tuple8<Integer, String, Double, Double, String, String, String, String>> execute(final String date) {
		Table lineitem = env.scan("lineitem")
				.filter("l_returnflag = 'R'");
		Table orders = env.scan("orders")
				.filter("o_orderdate.toDate >= '" + date + "'.toDate")
				.filter("o_orderdate.toDate < '" + date + "'.toDate + 3.months");
		Table customer = env.scan("customer");
		Table nation = env.scan("nation");

		Table res = customer.join(nation).where("c_nationkey = n_nationkey")
				.join(orders).where("c_custkey = o_custkey")
				.join(lineitem).where("l_orderkey = o_orderkey")
				.groupBy("c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment")
				.select("c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue, "
						+ "c_acctbal, n_name, c_address, c_phone, c_comment")
				.orderBy("revenue.desc");

		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple8<Integer, String, Double, Double, String, String, String, String>>() {
			})).map(new MapFunction<Tuple8<Integer, String, Double, Double, String, String, String, String>, Tuple8<Integer, String, Double, Double, String, String, String, String>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple8<Integer, String, Double, Double, String, String, String, String> map(final Tuple8<Integer, String, Double, Double, String, String, String, String> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private String getRandomDate() {
		int year = Utils.getRandomInt(1993, 1995);
		int month = Utils.getRandomInt(1, 12);
		if (month == 1 && year == 1993) {
			month = Utils.getRandomInt(2, 12);
		}
		String monthString;
		if (month < 10) {
			monthString = "-0" + month;
		} else {
			monthString = "-" + month;
		}
		return year + monthString + "-01";
	}

}
