package de.dima.tuberlin.bdapro.spark.tpch.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.bdapro.spark.tpch.Utils;
import de.tuberlin.dima.bdapro.spark.tpch.Utils.Nation;
import de.tuberlin.dima.bdapro.spark.tpch.batch.TableSourceProvider;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query1;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query2;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query10;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query11;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query6;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query7;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query8;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query9;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query5;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query12;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query15;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query18;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query19;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query20;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query21;


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
	public void Query2() {
		final Query2 q2 = new Query2(spark);
		final List<Row> result = q2.execute("BRASS", 15, "EUROPE");

		for (final Row elem : result) {

			if (Utils.convertToTwoDecimal(elem.getDouble(0)) == 9938.53 &&
					elem.getString(1).equals("Supplier#000005359") &&
					elem.getString(2).equals("UNITED KINGDOM") &&
					elem.getInt(3) == 185358 &&
					elem.getString(4).equals("Manufacturer#4") &&
					elem.getString(5).equals("QKuHYh,vZGiwu2FWEJoLDx04") &&
					elem.getString(6).equals("33-429-790-6131") &&
					elem.getString(7).equals("uriously regular requests hag") ) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query1 failed");
	}
	
	@Test
	public void Query5() {
		final Query5 q5 = new Query5(spark);
		final List<Row> result = q5.execute("ASIA", LocalDate.parse("1994-01-01"));

		for (final Row elem : result) {
			if (elem.getString(0).equals("INDONESIA") &&
					Utils.convertToTwoDecimal(elem.getDouble(1)) == 55502041.17) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query5 failed");
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

	@Test
	public void Query11() {
		final Query11 q11 = new Query11(spark, sf);
		final List<Row> result = q11.execute(Nation.GERMANY.getName(), 0.0001);

		for (final Row elem : result) {
			if (elem.getInt(0) == 129760 && 
					Utils.convertToTwoDecimal(elem.getDouble(1)) == 17538456.86) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query11 failed");

	}
	
	@Test
	public void Query12() {
		final Query12 q12 = new Query12(spark);
		final List<Row> result = q12.execute("MAIL", "SHIP", LocalDate.parse("1994-01-01"));

		for (final Row elem : result) {
			if (elem.getString(0).equals("MAIL") && 
					elem.getLong(1) == 6202 &&
					elem.getLong(2) == 9324) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query12 failed");
	}
	
	@Test
	public void Query15() {
		final Query15 q15 = new Query15(spark);
		final List<Row> result = q15.execute(LocalDate.parse("1996-01-01"));

		for (final Row elem : result) {
			if (elem.getInt(0) == 8449 &&
					elem.getString(1).equals("Supplier#000008449") && 
					elem.getString(2).equals("Wp34zim9qYFbVctdW") && 
					elem.getString(3).equals("20-469-856-8873") &&  
					Utils.convertToTwoDecimal(elem.getDouble(4)) == 1772627.21) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query15 failed");
	}
	
	@Test
	public void Query18() {
		final Query18 q18 = new Query18(spark);
		final List<Row> result = q18.execute(300);

		for (final Row elem : result) {
			if (elem.getString(0).equals("Customer#000128120") && 
					elem.getInt(1) == 128120 &&
					elem.getInt(2) == 4722021 &&
					elem.getString(3).equals("1994-04-07") &&
					Utils.convertToTwoDecimal(elem.getDouble(4)) == 544089.09 && 
					Utils.convertToTwoDecimal(elem.getDouble(5)) == 323.00) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query18 failed");
	}
	
	@Test
	public void Query19() {
		final Query19 q19 = new Query19(spark);
		final List<Row> result = q19.execute("Brand#12", "Brand#23", "Brand#34", 1, 10, 20);

		for (final Row elem : result) {
			if (Utils.convertToTwoDecimal(elem.getDouble(0)) == 3083843.06) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query19 failed");
	}
	
	@Test
	public void Query20() {
		final Query20 q20 = new Query20(spark);
		final List<Row> result = q20.execute("forest", LocalDate.parse("1994-01-01"), "CANADA");

		for (final Row elem : result) {
			if (elem.getString(0).equals("Supplier#000000020") && 
					elem.getString(1).equals("iybAE,RmTymrZVYaFZva2SH,j")) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query20 failed");
	}
	
	@Test
	public void Query21() {
		final Query21 q21 = new Query21(spark);
		final List<Row> result = q21.execute("SAUDI ARABIA");

		for (final Row elem : result) {
			System.out.println(elem.getString(0) + " -- " + elem.getLong(1));
			if (elem.getString(0).equals("Supplier#000002829") && 
					elem.getLong(1) == (long) 20) {
				assertEquals(0, 0);
				return;
			}
		}
		fail("Query21 failed");
	}

}
