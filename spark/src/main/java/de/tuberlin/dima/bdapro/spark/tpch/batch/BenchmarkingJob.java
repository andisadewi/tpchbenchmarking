package de.tuberlin.dima.bdapro.spark.tpch.batch;

import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query1;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("TPCH Spark Batch Benchmarking").master("local").getOrCreate();
		spark = TableSourceProvider.loadData(spark, "1.0");
		long start = System.currentTimeMillis();
		System.out.println("start: " + start);
		final Query1 q13 = new Query1(spark);
		long end = System.currentTimeMillis();
		System.out.println("end: " + end);
		System.out.println("diff: " + (end-start));

	}

}
