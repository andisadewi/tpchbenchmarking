package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import de.tuberlin.dima.bdapro.spark.tpch.PathConfig;

public class Query6 {

	public Query6() {
		SparkSession spark = SparkSession
				.builder()
				.appName("TPCH")
				.master("local")
				.getOrCreate();

		StructField[] schemas = new StructField[]{
				DataTypes.createStructField("orderkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("partkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("suppkey", DataTypes.IntegerType, false),
				DataTypes.createStructField("linenumber", DataTypes.IntegerType, false),
				DataTypes.createStructField("quantity", DataTypes.DoubleType, false),
				DataTypes.createStructField("extendedprice", DataTypes.DoubleType, false),
				DataTypes.createStructField("discount", DataTypes.DoubleType, false),
				DataTypes.createStructField("tax", DataTypes.DoubleType, false),
				DataTypes.createStructField("returnflag", DataTypes.StringType, false),
				DataTypes.createStructField("linestatus", DataTypes.StringType, false),
				DataTypes.createStructField("shipdate", DataTypes.StringType, false),
				DataTypes.createStructField("commitdate", DataTypes.StringType, false),
				DataTypes.createStructField("receiptdate", DataTypes.StringType, false),
				DataTypes.createStructField("shipinstruct", DataTypes.StringType, false),
				DataTypes.createStructField("shipmode", DataTypes.StringType, false),
				DataTypes.createStructField("comment", DataTypes.StringType, false)			
		};

		StructType schema = new StructType(schemas);
		Dataset<Row> df = spark.read()
				.option("delimiter", "|")
				.schema(schema)
				.csv(PathConfig.BASE_DIR + "1.0/" + PathConfig.LINEITEM);
		df.createOrReplaceTempView("lineitem");

		LocalDate dateRandom = LocalDate.parse("1994-01-01");
		LocalDate interval = dateRandom.plusYears(1);

		Dataset<Row> res = spark.sql("select sum(extendedprice*discount) as revenue "
				+ "from lineitem "
				+ "where shipdate >= '" + dateRandom.toString() + "' "
				+ "and shipdate < '" + interval.toString() + "' "
				+ "and discount between 0.05 and 0.07 "
				+ "and quantity < 24");
		//		res.show();

		spark.close();
	}

}
