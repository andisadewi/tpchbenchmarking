package de.tuberlin.dima.bdapro.tpch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple8;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.bdapro.tpch.queries.Query1;
import de.tuberlin.dima.bdapro.tpch.queries.Query10;
import de.tuberlin.dima.bdapro.tpch.queries.Query6;

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
	public void Query10() {
		final Query10 q10 = new Query10(env, "1.0");
		final List<Tuple8<Integer, String, Double, Double, String, String, String, String>> result = q10
				.execute("1993-10-01");

		final Tuple8<Integer, String, Double, Double, String, String, String, String> expected = new Tuple8<Integer, String, Double, Double, String, String, String, String>(
				57040, "Customer#000057040", 734235.24, 632.87, "JAPAN", "Eioyzjf4pp", "22-895-641-3466",
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
