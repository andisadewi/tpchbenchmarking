package de.tuberlin.dima.bdapro.tpch.queries;

import java.util.List;

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

	@SuppressWarnings("unchecked")
	public static <T extends Tuple> T keepOnlyTwoDecimals(final Tuple tuple) {
		final int size = tuple.getArity();
		for (int i = 0; i < size; i++) {
			if (tuple.getField(i) instanceof Double) {
				tuple.setField(Math.round((double) tuple.getField(i) * 100.0) / 100.0, i);
			}
		}
		return (T) tuple;
	}
}
