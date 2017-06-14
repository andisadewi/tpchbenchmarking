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
import de.tuberlin.dima.bdapro.flink.tpch.batch.queries.*;

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

		List<String> results = new ArrayList<String>();
		
		long start = System.currentTimeMillis();
		//final Query q1 = new Query1(tableEnv);
		//q2.execute();
		long end = System.currentTimeMillis();
		//results.add(" Query1 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		//final Query q2 = new Query2(tableEnv);
		//q2.execute();
		end = System.currentTimeMillis();
		//results.add(" Query2 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q3 = new Query3(tableEnv);
		q3.execute();
		end = System.currentTimeMillis();
		results.add(" Query3 execution time in miliseconds: " + (end - start) + "\r\n");
		
		 start = System.currentTimeMillis();
		final Query q4 = new Query4(tableEnv);
		q4.execute();
		end = System.currentTimeMillis();
		results.add(" Query4 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q5 = new Query5(tableEnv);
		q5.execute();
		end = System.currentTimeMillis();
		results.add(" Query5 execution time in miliseconds: " + (end - start) + "\r\n");
		
//		start = System.currentTimeMillis();
//		final Query q6 = new Query6(tableEnv);
//		q6.execute();
//		end = System.currentTimeMillis();
//		results.add(" Query6 execution time in miliseconds: " + (end - start) + "\r\n");
		
		 start = System.currentTimeMillis();
		final Query q7 = new Query7(tableEnv);
		q7.execute();
		end = System.currentTimeMillis();
		results.add(" Query7 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q8 = new Query8(tableEnv);
		q8.execute();
		end = System.currentTimeMillis();
		results.add(" Query8 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q9 = new Query9(tableEnv);
		q9.execute();
		end = System.currentTimeMillis();
		results.add(" Query9 execution time in miliseconds: " + (end - start) + "\r\n");
		
		 start = System.currentTimeMillis();
		final Query q10 = new Query10(tableEnv);
		q10.execute();
		end = System.currentTimeMillis();
		results.add(" Query10 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q11 = new Query11(tableEnv, "1.0");
		q11.execute();
		end = System.currentTimeMillis();
		results.add(" Query11 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q12 = new Query12(tableEnv);
		q12.execute();
		end = System.currentTimeMillis();
		results.add(" Query12 execution time in miliseconds: " + (end - start) + "\r\n");
		
		 start = System.currentTimeMillis();
		final Query q13 = new Query13(tableEnv);
		q13.execute();
		end = System.currentTimeMillis();
		results.add(" Query13 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q14 = new Query14(tableEnv);
		q14.execute();
		end = System.currentTimeMillis();
		results.add(" Query14 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q15 = new Query15(tableEnv);
		q15.execute();
		end = System.currentTimeMillis();
		results.add(" Query15 execution time in miliseconds: " + (end - start) + "\r\n");
		
//		start = System.currentTimeMillis();
//		final Query q16 = new Query16(tableEnv);
//		q16.execute();
//		end = System.currentTimeMillis();
//		results.add(" Query16 execution time in miliseconds: " + (end - start) + "\r\n");
		
//		start = System.currentTimeMillis();
//		final Query q17 = new Query17(tableEnv);
//		q17.execute();
//		end = System.currentTimeMillis();
//		results.add(" Query17 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q18 = new Query18(tableEnv);
		q18.execute();
		end = System.currentTimeMillis();
		results.add(" Query18 execution time in miliseconds: " + (end - start) + "\r\n");
		
		 start = System.currentTimeMillis();
		final Query q19 = new Query19(tableEnv);
		q19.execute();
		end = System.currentTimeMillis();
		results.add(" Query19 execution time in miliseconds: " + (end - start) + "\r\n");
		
		start = System.currentTimeMillis();
		final Query q20 = new Query20(tableEnv);
		q20.execute();
		end = System.currentTimeMillis();
		results.add(" Query20 execution time in miliseconds: " + (end - start) + "\r\n");
		
//		start = System.currentTimeMillis();
//		final Query q21 = new Query21(tableEnv);
//		q21.execute();
//		end = System.currentTimeMillis();
//		results.add(" Query21 execution time in miliseconds: " + (end - start) + "\r\n");
		
//		 start = System.currentTimeMillis();
//		final Query q22 = new Query22(tableEnv);
//		q22.execute();
//		end = System.currentTimeMillis();
//		results.add(" Query22 execution time in miliseconds: " + (end - start) + "\r\n");

		////////////////////////WRITE OUTPUT TO FILE ///////////////////////////////
		try {
			FileWriter writer = new FileWriter("FlinkBatchOutput.txt", true);
			for (String str : results) {
				writer.write(str);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

}
