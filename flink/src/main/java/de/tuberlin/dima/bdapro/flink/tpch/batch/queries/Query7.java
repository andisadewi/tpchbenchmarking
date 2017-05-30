package de.tuberlin.dima.bdapro.flink.tpch.batch.queries;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import de.tuberlin.dima.bdapro.flink.tpch.Utils.Nation;

public class Query7 extends Query {

	public Query7(final BatchTableEnvironment env) {
		super(env);
	}

	@Override
	public List<Tuple4<String, String, Integer, Double>> execute() {
		String[] randNations = getTwoRandomNations();
		return execute(randNations[0], randNations[1]);
	}

	public List<Tuple4<String, String, Integer, Double>> execute(final String nation1, final String nation2) {
		return null;
	}

	private String[] getTwoRandomNations() {
		String nation1 = Nation.getRandomNation();
		String nation2 = Nation.getRandomNation();
		if (nation1.equals(nation2)) {
			return getTwoRandomNations();
		} else {
			return new String[] { nation1, nation2 };
		}
	}

}
