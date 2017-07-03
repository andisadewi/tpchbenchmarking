package de.tuberlin.dima.bdapro.spark.tpch.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class BenchmarkingJob {

	public static void main(final String[] args) {
		// TODO change master if run on cluster
		SparkSession spark = SparkSession.builder()
				.appName("TPCH Spark Streaming Benchmarking").master("local").getOrCreate();
		
		// Subscribe to 1 topic
		Dataset<Row> ds1 = spark
		  .readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("subscribe", "lineitem")
		  .load();
		
		ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
		
		// Split the lines into words
		Dataset<String> words = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")		  
		  .flatMap(
		    new FlatMapFunction<Row, String>() {
		      @Override
		      public Iterator<String> call(final Row x) {
		        return Arrays.asList(((String) x.get(1)).split("\\|")).iterator();
		      }
		    }, Encoders.STRING());
		
		// Generate running word count
		Dataset<Row> wordCounts = words.groupBy("value").count();
		
		// Start running the query that prints the running counts to the console
		StreamingQuery query = wordCounts.writeStream()
		  .outputMode("complete")
		  .format("console")
		  .start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
