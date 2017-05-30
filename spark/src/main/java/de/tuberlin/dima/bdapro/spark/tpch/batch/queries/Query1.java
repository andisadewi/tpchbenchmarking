package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;

public class Query1 extends Query{

	public Query1(final SparkSession spark) {
		super(spark);
	}

	@Override
	public List<Row> execute() {
		return execute(Utils.getRandomInt(60, 120));
	}

	public List<Row> execute(final int delta) {
		String dateThreshold = LocalDate.parse("1998-12-01").minusDays(delta).toString();

		return spark.sql("select returnflag, "
				+ "linestatus, "
				+ "sum(quantity) as sum_qty, "
				+ "sum(extendedprice) as sum_base_price, "
				+ "sum(extendedprice*(1-discount)) as sum_disc_price, "
				+ "sum(extendedprice*(1-discount)*(1+tax)) as sum_charge, "
				+ "avg(quantity) as avg_qty, "
				+ "avg(extendedprice) as avg_price, "
				+ "avg(discount) as avg_disc, "
				+ "count(*) as count_order "
				+ "from lineitem "
				+ "where shipdate <= '" + dateThreshold + "' "
				+ "group by returnflag, linestatus "
				+ "order by returnflag, linestatus").collectAsList();

	}

}
