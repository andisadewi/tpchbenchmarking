package de.tuberlin.dima.bdapro.flink.tpch.batch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.TableSourceProvider;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query;
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.Query2;

/**
 * How to use: ./bin/flink run benchmarkingJob.jar <path//to//the//testDB>
 * <//scaleFactor//> example: ./bin/flink run benchmarkingJob.jar
 * /home/ubuntu/tpch/dbgen/testdata 1.0
 *
 */

public class BenchmarkingJob {

	public static void main(final String[] args) {
		//////////////////////// ARGUMENT PARSING ///////////////////////////////
		if (args.length <= 0 || args.length > 2) {
			throw new IllegalArgumentException(
					"Please input the path to the directory where the test databases are located.");
		}
		String path = args[0];
		try {
			File file = new File(path);
			if (!(file.isDirectory() && file.exists())) {
				throw new IllegalArgumentException(
						"Please give a valid path to the directory where the test databases are located.");
			}
		} catch (Exception ex) {
			throw new IllegalArgumentException(
					"Please give a valid path to the directory where the test databases are located.");
		}

		String sf = args[1];
		try {
			Double.parseDouble(sf);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Please give a valid scale factor.");
		}

		////////////////////////QUERIES ///////////////////////////////
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableSourceProvider provider = new TableSourceProvider();
		provider.setBaseDir(path);
		BatchTableEnvironment tableEnv = provider.loadDataBatch(env, sf);

		List<Long> results = new ArrayList<Long>();
		long start = System.currentTimeMillis();
		final Query q13 = new Query2(tableEnv);
		q13.execute();
		long end = System.currentTimeMillis();
		results.add(end - start);

		////////////////////////WRITE OUTPUT TO FILE ///////////////////////////////
		try {
			FileWriter writer = new FileWriter("FlinkBatchOutput.txt", true);
			for (long str : results) {
				writer.write((int) str);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

}
