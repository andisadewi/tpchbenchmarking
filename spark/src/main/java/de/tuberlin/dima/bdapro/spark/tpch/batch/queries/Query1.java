package de.tuberlin.dima.bdapro.spark.tpch.batch.queries;

import java.time.LocalDate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import de.tuberlin.dima.bdapro.spark.tpch.PathConfig;

public class Query1 {

	public static void main(final String[] args) {
		new Query1();
	}

	public Query1() {
		//		SparkConf conf = new SparkConf().setAppName("TPCH").setMaster("local"); // TODO remove local later		

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

		String dateThreshold = LocalDate.parse("1998-12-01").minusDays(90).toString();

		Dataset<Row> res = spark.sql("select returnflag, "
				+ "linestatus, "
				+ "sum(quantity) as sum_qty, "
				+ "sum(extendedprice) as sum_base_price, "
				+ "sum(extendedprice*(1-discount)) as sum_disc_price, "
				+ "sum(extendedprice*(1-discount)*(1+tax)) as sum_charge, "
				+ "avg(quantity) as avg_qty, "
				+ "avg(extendedprice) as avg_price, "
				+ "avg(discount) as avg_disc, "
				+ "count(*) as count_order "
				+ "from lineitem "
				+ "where shipdate <= '" + dateThreshold + "' "
				+ "group by returnflag, linestatus "
				+ "order by returnflag, linestatus");
		//		res.show();
		spark.close();
	}

}
