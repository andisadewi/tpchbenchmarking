package de.tuberlin.dima.bdapro.tpch;

import org.apache.flink.api.java.ExecutionEnvironment;

import de.tuberlin.dima.bdapro.tpch.queries.Query;
import de.tuberlin.dima.bdapro.tpch.queries.Query1;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final Query q1 = new Query1(env, "1.0");
		q1.execute();

	}

}
