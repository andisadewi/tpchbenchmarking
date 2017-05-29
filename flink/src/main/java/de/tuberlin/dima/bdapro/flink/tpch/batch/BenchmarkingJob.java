package de.tuberlin.dima.bdapro.flink.tpch.batch;

import org.apache.flink.api.java.ExecutionEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query6;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		long start = System.currentTimeMillis();
		System.out.println("start: " + start);
		final Query q13 = new Query6(env, "1.0");
		q13.execute();
		long end = System.currentTimeMillis();
		System.out.println("end: " + end);
		System.out.println("diff: " + (end-start));
	}

}
