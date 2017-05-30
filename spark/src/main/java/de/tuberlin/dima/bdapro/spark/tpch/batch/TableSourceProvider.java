package de.tuberlin.dima.bdapro.spark.tpch.batch;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import de.tuberlin.dima.bdapro.spark.tpch.PathConfig;

public class TableSourceProvider {

	public static SparkSession loadData(final SparkSession spark, final String sf) {
		StructField[] lineitem = new StructField[] {
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
				DataTypes.createStructField("comment", DataTypes.StringType, false) };

		spark.read().option("delimiter", "|").schema(new StructType(lineitem))
		.csv(PathConfig.BASE_DIR + sf + "/" + PathConfig.LINEITEM)
		.createOrReplaceTempView("lineitem");

		return spark;
	}

}
