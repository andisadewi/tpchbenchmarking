package de.tuberlin.dima.bdapro.flink.tpch.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.bdapro.flink.tpch.batch.TableSourceProvider;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query1;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query6;

public class QueriesTest {

	private BatchTableEnvironment tableEnv;
	private boolean loadedData = false;

	@Before
	public void setUp() throws Exception {
		if(!loadedData){
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			tableEnv = TableSourceProvider.loadData(env, "1.0");
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
}
