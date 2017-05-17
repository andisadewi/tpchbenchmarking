package de.tuberlin.dima.bdapro.tpch.queries;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple7;

import de.tuberlin.dima.bdapro.tpch.PathConfig;

public class Query1 {

	private ExecutionEnvironment env;
	private String sf;

	public Query1(ExecutionEnvironment env, String sf) {
		this.env = env;
		this.sf = sf;

	}

	private DataSource<Tuple7<Integer, Double, Double, Double, String, String, String>> readLineitem() {
		CsvReader source = null;
		switch(sf){
		case("1.0"):
			source = env.readCsvFile(PathConfig.LINEITEM_1);
			break;
		}
		
		return source.fieldDelimiter("|")
		.includeFields("0000111111100000")
		.types(Integer.class, Double.class, Double.class, Double.class, String.class, String.class, String.class);
		
	}

}
