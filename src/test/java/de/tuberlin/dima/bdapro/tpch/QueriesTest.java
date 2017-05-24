package de.tuberlin.dima.bdapro.tpch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.time.LocalDate;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.bdapro.tpch.Config.Nation;
import de.tuberlin.dima.bdapro.tpch.queries.Query;
import de.tuberlin.dima.bdapro.tpch.queries.Query1;
import de.tuberlin.dima.bdapro.tpch.queries.Query10;
import de.tuberlin.dima.bdapro.tpch.queries.Query2;
import de.tuberlin.dima.bdapro.tpch.queries.Query3;
import de.tuberlin.dima.bdapro.tpch.queries.Query5;
import de.tuberlin.dima.bdapro.tpch.queries.Query6;
import de.tuberlin.dima.bdapro.tpch.queries.Query7;
import de.tuberlin.dima.bdapro.tpch.queries.Query8;

import de.tuberlin.dima.bdapro.tpch.queries.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.junit.Before;
import org.junit.Test;

public class QueriesTest {

	private ExecutionEnvironment env;

	@Before
	public void setUp() throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();
	}

	@Test
	public void Query1() {
		final Query1 q1 = new Query1(env, "1.0");
		final List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> result = q1
				.execute(90);

		final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> expected = new Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>(
				"A", "F", 37734107.00, 56586554400.73, 53758257134.87, 55909065222.83, 25.52, 38273.13, .05, 1478493);

		for (final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query1 failed");

	}

	@Test
	public void Query2() {
		final Query2 q2 = new Query2(env, "1.0");
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
	public void Query3() {
		final Query3 q3 = new Query3(env, "1.0");
		q3.setSegment("BUILDING");
		q3.setDate(LocalDate.of(1995,03,15));

		final List<Tuple4<Long, Double, String, Long>> result = q3.execute();
		Integer i = 2456423;
		Integer j = 0;
		final Tuple4<Long, Double, String, Long> validation = new Tuple4<Long, Double, String, Long>(
				i.longValue(), 406181.01, "1995-03-05", j.longValue());

		for (final Tuple4<Long, Double, String, Long> elem : result) {
			if (elem.equals(validation)) {
				assertEquals(validation, elem);
				return;
			}
		}
		fail("Query3 failed");

	}
	@Test
	public void Query4() {
		final Query4 q4 = new Query4(env, "1.0");
		q4.setDate(LocalDate.of(1993,07,01));

		final List<Tuple2<String,Long>> result = q4.execute();
		Integer i = 10594;
		final Tuple2<String, Long> validation = new Tuple2<String, Long>(
				"1-URGENT", i.longValue());

		for (final Tuple2<String, Long> elem : result) {
			if (elem.equals(validation)) {
				assertEquals(validation, elem);
				return;
			}
		}
		fail("Query4 failed");

	}
	@Test
	public void Query5() {
		final Query5 q5 = new Query5(env, "1.0");
		final List<Tuple2<String, Double>> result = q5.execute("ASIA", "1994-01-01");

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
		final Query6 q6 = new Query6(env, "1.0");
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
		final Query7 q7 = new Query7(env, "1.0");
		final List<Tuple4<String, String, Integer, Double>> result = q7.execute(Nation.FRANCE.getName(), Nation.GERMANY.getName());

		final Tuple4<String, String, Integer, Double> expected = new Tuple4<String, String, Integer, Double>(Nation.FRANCE.getName(),
				Nation.GERMANY.getName(), 1995, 54639732.73);

		for (final Tuple4<String, String, Integer, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query7 failed");

	}

	@Test
	public void Query8() {
		final Query8 q8 = new Query8(env, "1.0");
		final List<Tuple2<Integer, Double>> result = q8.execute(Nation.BRAZIL.getName(), Nation.BRAZIL.getRegion(), Query.getRandomType());

		final Tuple2<Integer, Double> expected = new Tuple2<Integer, Double>(1995, 0.03);

		for (final Tuple2<Integer, Double> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query8 failed");

	}

	@Test
	public void Query10() {
		final Query10 q10 = new Query10(env, "1.0");
		final List<Tuple8<Integer, String, Double, Double, String, String, String, String>> result = q10
				.execute("1993-10-01");

		final Tuple8<Integer, String, Double, Double, String, String, String, String> expected = new Tuple8<Integer, String, Double, Double, String, String, String, String>(
				57040, "Customer#000057040", 734235.24, 632.87, Nation.JAPAN.getName(), "Eioyzjf4pp", "22-895-641-3466",
				"sits. slyly regular requests sleep alongside of the regular inst");

		for (final Tuple8<Integer, String, Double, Double, String, String, String, String> elem : result) {
			if (elem.equals(expected)) {
				assertEquals(expected, elem);
				return;
			}
		}
		fail("Query10 failed");

	}
}
