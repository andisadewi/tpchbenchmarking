package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils;
import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

public class Query5 extends Query {

	public Query5(BatchTableEnvironment env) {
		super(env);
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Tuple2<String, Double>> execute() {
		return execute(Nation.getRandomRegion(), LocalDate.parse(Utils.getRandomInt(1993, 1997) + "-01-01"));
	}
	
	public List<Tuple2<String, Double>> execute(String rndRegion, LocalDate rndDate) {
		
		String SQLQuery = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
				+ "FROM customer, orders, lineitem, supplier, nation, region "
				+ "WHERE c_custkey = o_custkey and l_orderkey = o_orderkey "
				+ "and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey "
				+ "and r_name = '" + rndRegion +"' and "
				+ "o_orderdate >= '" + rndDate.toString() +"' and "
				+ "o_orderdate < '" + rndDate.plusYears(1).toString() + "' " 
				+ "GROUP BY n_name ORDER BY revenue desc";
		
		Table res = env.sql(SQLQuery);
		
		try {
			return env.toDataSet(res, TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
			})).map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> map(final Tuple2<String, Double> value) throws Exception {
					return Utils.keepOnlyTwoDecimals(value);
				}
			}).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
