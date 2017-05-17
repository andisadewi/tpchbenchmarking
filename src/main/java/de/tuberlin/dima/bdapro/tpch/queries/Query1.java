package de.tuberlin.dima.bdapro.tpch.queries;

import java.time.LocalDate;
import java.util.Date;
import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple7;

import de.tuberlin.dima.bdapro.tpch.PathConfig;

public class Query1 {

	private ExecutionEnvironment env;
	private String sf;

	public Query1(ExecutionEnvironment env, String sf) {
		this.env = env;
		this.sf = sf;

		final int delta = getDelta();
		final LocalDate thresholdDate = LocalDate.of(1998, 12, 1);

		DataSet<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>> lineitems = readLineitem();

		lineitems.filter(new FilterFunction<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>>() {

			@Override
			public boolean filter(Tuple7<Integer, Double, Double, Double, String, String, LocalDate> arg0)
					throws Exception {
				return false;
			}
		});

	}

	private DataSet<Tuple7<Integer, Double, Double, Double, String, String, LocalDate>> readLineitem() {
		CsvReader source = null;
		switch (sf) {
		case ("1.0"):
			source = env.readCsvFile(PathConfig.LINEITEM_1);
			break;
		default:
			source = env.readCsvFile(PathConfig.LINEITEM_1);
			break;
		}

		return source.fieldDelimiter("|").includeFields("0000111111100000").types(Integer.class, Double.class,
				Double.class, Double.class, String.class, String.class, LocalDate.class);
	}

	private int getDelta() {
		Random rand = new Random();
		return 60 + rand.nextInt((120 - 60) + 1);
	}

}
