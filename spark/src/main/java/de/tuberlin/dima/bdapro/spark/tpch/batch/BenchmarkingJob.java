package de.tuberlin.dima.bdapro.spark.tpch.batch;

import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query6;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		long start = System.currentTimeMillis();
		System.out.println("start: " + start);
		final Query6 q13 = new Query6();
		long end = System.currentTimeMillis();
		System.out.println("end: " + end);
		System.out.println("diff: " + (end-start));

	}

}
