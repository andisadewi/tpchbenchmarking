package de.tuberlin.dima.bdapro.spark.tpch.batch;

import org.apache.spark.sql.SparkSession;

import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query;
import de.tuberlin.dima.bdapro.spark.tpch.batch.queries.Query8;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("TPCH Spark Batch Benchmarking").master("local").getOrCreate();
		spark = TableSourceProvider.loadData(spark, "1.0");
		long start = System.currentTimeMillis();
		System.out.println("start: " + start);
		final Query q13 = new Query8(spark);
		long end = System.currentTimeMillis();
		System.out.println("end: " + end);
		System.out.println("diff: " + (end-start));

	}

}
