package de.tuberlin.dima.bdapro.flink.tpch.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.TableSourceProvider;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query9;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		String sf = "1.0";
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableSourceProvider.loadDataBatch(env, sf);
		long start = System.currentTimeMillis();
		System.out.println("start: " + start);
		final Query q13 = new Query9(tableEnv);
		q13.execute();
		long end = System.currentTimeMillis();
		System.out.println("end: " + end);
		System.out.println("diff: " + (end-start));
	}

}
