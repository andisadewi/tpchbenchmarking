package de.tuberlin.dima.bdapro.tpch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple10;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.bdapro.tpch.queries.Query1;

public class QueriesTest {

	private ExecutionEnvironment env;

	@Before
	public void setUp() throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();
	}

	@Test
	public void Query1() {
		final Query1 q1 = new Query1(env, "1.0");
		q1.setDelta(90);
		final List<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> result = q1
				.execute();

		final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> validation = new Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>(
				"A", "F", 37734107.00, 56586554400.73, 53758257134.87, 55909065222.83, 25.52, 38273.13, .05, 1478493);

		for (final Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> elem : result) {
			if (elem.equals(validation)) {
				assertEquals(validation, elem);
				return;
			}
		}
		fail("Query1 failed");

	}

}
