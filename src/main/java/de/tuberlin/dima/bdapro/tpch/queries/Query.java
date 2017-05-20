package de.tuberlin.dima.bdapro.tpch.queries;

import java.util.List;
import java.util.Random;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple;

import de.tuberlin.dima.bdapro.tpch.Config;

public abstract class Query {

	private final ExecutionEnvironment env;
	private final String sf;

	public Query(final ExecutionEnvironment env, final String sf) {
		this.env = env;
		this.sf = sf;
	}

	/**
	 * Get the appropriate CSV Reader.
	 * 
	 * @param tableName
	 *            the table name that should be fetched. See Config for a
	 *            correct table name
	 * @return the CSV Reader
	 */
	protected CsvReader getCSVReader(final String tableName) {
		return env.readCsvFile(Config.BASE_DIR + sf + "/" + tableName);
	}

	public abstract List<? extends Tuple> execute();

	/**
	 * Check all double fields in input tuple and keep only two decimal place.
	 * 
	 * @param tuple
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Tuple> T keepOnlyTwoDecimals(final Tuple tuple) {
		final int size = tuple.getArity();
		for (int i = 0; i < size; i++) {
			if (tuple.getField(i) instanceof Double) {
				tuple.setField(convertToTwoDecimal((double) tuple.getField(i)), i);
			}
		}
		return (T) tuple;
	}

	public static double convertToTwoDecimal(final double value) {
		return Math.round(value * 100.0) / 100.0;
	}

	/**
	 * Get a random integer between two values (both are inclusive).
	 * 
	 * @param upperLimit
	 * @param bottomLimit
	 * @return
	 */
	public static int getRandomInt(final int bottomLimit, final int upperLimit) {
		final Random rand = new Random();
		return bottomLimit + rand.nextInt((upperLimit - bottomLimit) + 1);
	}

	/**
	 * Get a random double between two values (both are inclusive).
	 * 
	 * @param upperLimit
	 * @param bottomLimit
	 * @return
	 */
	public static double getRandomDouble(final double bottomLimit, final double upperLimit) {
		final Random rand = new Random();
		return bottomLimit + (upperLimit - bottomLimit) * rand.nextDouble();
	}
}
