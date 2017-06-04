package de.tuberlin.dima.bdapro.flink.tpch.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.bdapro.flink.tpch.TableSourceProvider;
import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query1;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.*;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query10;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query11;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query6;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query7;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query8;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query9;

public class QueriesTest {

	private BatchTableEnvironment tableEnv;
	private boolean loadedData = false;
	private String sf = "1.0";

	@Before
	public void setUp() throws Exception {
		if(!loadedData){
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			tableEnv = TableSourceProvider.loadDataBatch(env, sf);
			loadedData = true;
		}	
	}

	@Test
	public void Query1() {
		final Query1 q1 = new Query1(tableEnv);
		final List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>> result = q1
				.execute(90);

		final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> expected = new Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long>(
				"A", "F", 37734107.00, 56586554400.73, 53758257134.87, 55909065222.83, 25.52, 38273.13, .05, (long)1478493);

		for (final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Long> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query1 failed");

	}
	
	@Test
	public void Query2() {
		final Query2 q2 = new Query2(tableEnv);
		final  List<Tuple8<Double, String, String, Integer, String, String, String, String>> result = q2.execute("BRASS", 15, "EUROPE");

		final Tuple8<Double, String, String, Integer, String, String, String, String> expected =
				new Tuple8<Double, String, String, Integer, String, String, String, String>(
						9938.53, "Supplier#000005359", "UNITED KINGDOM", 185358, "Manufacturer#4", "QKuHYh,vZGiwu2FWEJoLDx04", "33-429-790-6131", "uriously regular requests hag");

		for (final Tuple8<Double, String, String, Integer, String, String, String, String> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query2 failed");

	}
	
	@Test
	public void Query5() {
		final Query5 q5 = new Query5(tableEnv);
		final List<Tuple2<String, Double>> result = q5.execute("ASIA", LocalDate.parse("1994-01-01"));

		final Tuple2<String, Double> expected = new Tuple2<String, Double>("INDONESIA", 55502041.17);

		for (final Tuple2<String, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query5 failed");

	}

	@Test
	public void Query6() {
		final Query6 q6 = new Query6(tableEnv);
		final List<Tuple1<Double>> result = q6.execute("1994-01-01", 0.06, 24);

		final Tuple1<Double> expected = new Tuple1<Double>(123141078.23);

		for (final Tuple1<Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query6 failed");

	}

	@Test
	public void Query7() {
		final Query7 q7 = new Query7(tableEnv);
		final List<Tuple4<String, String, Long, Double>> result = q7.execute(Nation.FRANCE.getName(), Nation.GERMANY.getName());

		final Tuple4<String, String, Long, Double> expected = new Tuple4<String, String, Long, Double>(Nation.FRANCE.getName(),
				Nation.GERMANY.getName(), (long)1995, 54639732.73);

		for (final Tuple4<String, String, Long, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query7 failed");

	}

	@Test
	public void Query8() {
		final Query8 q8 = new Query8(tableEnv);
		final List<Tuple2<Long, Double>> result = q8.execute(Nation.BRAZIL.getName(), Nation.BRAZIL.getRegion(), "ECONOMY ANODIZED STEEL");

		final Tuple2<Long, Double> expected = new Tuple2<Long, Double>((long)1995, 0.03);

		for (final Tuple2<Long, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query8 failed");

	}

	@Test
	public void Query9() {
		final Query9 q9 = new Query9(tableEnv);
		final List<Tuple3<String, Long, Double>> result = q9.execute("green");

		// value 31342867.24
		final Tuple3<String, Long, Double> expected = new Tuple3<String, Long, Double>(Nation.ALGERIA.getName(), (long)1998, 27136900.18);

		for (final Tuple3<String, Long, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query9 failed");
	}

	@Test
	public void Query10() {
		final Query10 q10 = new Query10(tableEnv);
		final List<Tuple8<Integer, String, Double, Double, String, String, String, String>> result = q10
				.execute("1993-10-01");

		final Tuple8<Integer, String, Double, Double, String, String, String, String> expected = new Tuple8<Integer, String, Double, Double, String, String, String, String>(
				57040, "Customer#000057040", 734235.25, 632.87, Nation.JAPAN.getName(), "Eioyzjf4pp", "22-895-641-3466",
				"sits. slyly regular requests sleep alongside of the regular inst");

		for (final Tuple8<Integer, String, Double, Double, String, String, String, String> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query10 failed");

	}

	@Test
	public void Query11() {
		final Query11 q11 = new Query11(tableEnv, sf);
		final List<Tuple2<Integer, Double>> result = q11.execute(Nation.GERMANY.getName(), 0.0001);

		final Tuple2<Integer, Double> expected = new Tuple2<Integer, Double>(129760, 17538456.86);

		for (final Tuple2<Integer, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query11 failed");

	}
	
	@Test
	public void Query15() {
		final Query15 q15 = new Query15(tableEnv);
		final List<Tuple5<Integer, String, String, String, Double>> result = q15.execute(LocalDate.parse("1996-01-01"));

		final Tuple5<Integer, String, String, String, Double> expected =
				new Tuple5<Integer, String, String, String, Double>
				(8449, "Supplier#000008449", "Wp34zim9qYFbVctdW", "20-469-856-8873", 1772627.21);

		for (final Tuple5<Integer, String, String, String, Double> elem : result) {
            if (elem.equals(expected)) {
                assertEquals(expected, elem);
                return;
            }
        }
        fail("Query15 failed");
    }
	
	@Test
	public void Query18() {
		final Query18 q18 = new Query18(tableEnv);
		final List<Tuple6<String, Integer, Integer, String, Double, Double>> result = q18.execute(300);
		final Tuple6<String, Integer, Integer, String, Double, Double> expected =
				new Tuple6<String, Integer, Integer, String, Double, Double>
				("Customer#000128120", 128120, 4722021, "1994-04-07", 544089.09, 323.00);

		for (final Tuple6<String, Integer, Integer, String, Double, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query18 failed");
	}
	
	@Test
	public void Query19() {
		final Query19 q19 = new Query19(tableEnv);
		final List<Tuple1<Double>> result = q19.execute("Brand#12", "Brand#23", "Brand#34", 1, 10, 20);

		final Tuple1<Double> expected = new Tuple1<Double>(3083843.06);
		
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);
		
		
		for (final Tuple1<Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query19 failed");

	}
}
