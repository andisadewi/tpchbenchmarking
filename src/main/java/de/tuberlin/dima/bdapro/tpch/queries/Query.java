package de.tuberlin.dima.bdapro.tpch.queries;

import java.util.ArrayList;
import java.util.Arrays;
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

	private static final List<String> SEGMENTS = new ArrayList<>(Arrays.asList("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY","HOUSEHOLD"));

	private static final List<String> TYPE_SYL3 = new ArrayList<>(Arrays.asList("TIN", "NICKEL", "BRASS", "STEEL", "COPPER"));

	private static final List<String> TYPE_SYL2 = new ArrayList<>(Arrays.asList("ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"));

	private static final List<String> TYPE_SYL1 = new ArrayList<>(Arrays.asList("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"));

	private static String getRandomElementFromList(final List<String> list){
		Random rand = new Random();
		return list.get(rand.nextInt(list.size()));
	}

	public static String getRandomSegment() {
		return getRandomElementFromList(SEGMENTS);
	}

	public static String getRandomType() {
		return getRandomElementFromList(TYPE_SYL1) + " " + getRandomElementFromList(TYPE_SYL2) + " " + getRandomElementFromList(TYPE_SYL3);
	}

	public static String getRandomTypeSyl1() {
		return getRandomElementFromList(TYPE_SYL1);
	}

	public static String getRandomTypeSyl2() {
		return getRandomElementFromList(TYPE_SYL2);
	}

	public static String getRandomTypeSyl3() {
		return getRandomElementFromList(TYPE_SYL3);
	}


}
