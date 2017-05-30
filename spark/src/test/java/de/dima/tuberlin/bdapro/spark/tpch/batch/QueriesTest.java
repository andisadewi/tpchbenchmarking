package de.dima.tuberlin.bdapro.spark.tpch.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.Utils.Nation;
import de.tuberlin.dima.bdapro.spark.tpch.batch.TableSourceProvider;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query1;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query10;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query6;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query7;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query8;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query9;

public class QueriesTest {

	private SparkSession spark;
	private boolean loadedData = false;
	private String sf = "1.0";

	@Before
	public void setUp() throws Exception {
		if (!loadedData) {
			spark = SparkSession.builder().appName("TPCH Spark Batch Benchmarking").master("local").getOrCreate();
			spark = TableSourceProvider.loadData(spark, sf);
			loadedData = true;
		}
	}

	@Test
	public void Query1() {
		final Query1 q1 = new Query1(spark);
		final List<Row> result = q1.execute(90);

		for (final Row elem : result) {

			if (elem.getString(0).equals("A") &&
					elem.getString(1).equals("F") &&
					Utils.convertToTwoDecimal(elem.getDouble(2)) == 37734107.00 &&
					Utils.convertToTwoDecimal(elem.getDouble(3)) == 56586554400.73 &&
					Utils.convertToTwoDecimal(elem.getDouble(4)) == 53758257134.87 && 
					Utils.convertToTwoDecimal(elem.getDouble(5)) == 55909065222.83 && 
					Utils.convertToTwoDecimal(elem.getDouble(6)) == 25.52 && 
					Utils.convertToTwoDecimal(elem.getDouble(7)) == 38273.13 && 
					Utils.convertToTwoDecimal(elem.getDouble(8)) == 0.05 && 
					elem.getLong(9) == 1478493) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query1 failed");

	}

	@Test
	public void Query6() {
		final Query6 q6 = new Query6(spark);
		final List<Row> result = q6.execute("1994-01-01", 0.06, 24);

		for (final Row elem : result) {
			if (Utils.convertToTwoDecimal(elem.getDouble(0)) == 123141078.23) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query6 failed");

	}

	@Test
	public void Query7() {
		final Query7 q7 = new Query7(spark);
		final List<Row> result = q7.execute(Nation.FRANCE.getName(), Nation.GERMANY.getName());

		for (final Row elem : result) {

			if (elem.getString(0).equals("FRANCE") &&
					elem.getString(1).equals("GERMANY") && 
					elem.getString(2).equals("1995") && 
					Utils.convertToTwoDecimal(elem.getDouble(3)) == 54639732.73) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query7 failed");

	}

	@Test
	public void Query8() {
		final Query8 q8 = new Query8(spark);
		final List<Row> result = q8.execute(Nation.BRAZIL.getName(), Nation.BRAZIL.getRegion(), "ECONOMY ANODIZED STEEL");

		for (final Row elem : result) {
			if (elem.getInt(0) == 1995 &&
					Utils.convertToTwoDecimal(elem.getDouble(1)) == 0.03) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query8 failed");

	}

	@Test
	public void Query9() {
		final Query9 q9 = new Query9(spark);
		final List<Row> result = q9.execute("green");

		// value 31342867.24
		for (final Row elem : result) {

			if (elem.getString(0).equals("ALGERIA") &&
					elem.getInt(1) == 1998 && 
					Utils.convertToTwoDecimal(elem.getDouble(2)) == 27136900.18) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query9 failed");
	}

	@Test
	public void Query10() {
		final Query10 q10 = new Query10(spark);
		final List<Row> result = q10.execute("1993-10-01");

		for (final Row elem : result) {
			//			System.out.println(elem.toString());
			if (elem.getInt(0) == 57040 && 
					elem.getString(1).equals("Customer#000057040") &&
					Utils.convertToTwoDecimal(elem.getDouble(2)) == 734235.25 && 
					Utils.convertToTwoDecimal(elem.getDouble(3)) == 632.87 && 
					elem.getString(4).equals("JAPAN") && 
					elem.getString(5).equals("Eioyzjf4pp") &&
					elem.getString(6).equals("22-895-641-3466") &&
					elem.getString(7).equals("sits. slyly regular requests sleep alongside of the regular inst")) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query10 failed");

	}

}
