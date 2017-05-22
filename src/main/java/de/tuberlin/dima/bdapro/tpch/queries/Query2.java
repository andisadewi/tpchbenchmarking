package de.tuberlin.dima.bdapro.tpch.queries;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;

import de.tuberlin.dima.bdapro.tpch.PathConfig;

// Minimum Cost Supplier Query (Q2) -- TPC-H

public class Query2 {
	private ExecutionEnvironment env;
	private String sf;
	
	//List of type and region, we are going to select one randomly from the lists for the query.
	private List<String> typeList = new ArrayList<>(Arrays.asList("TIN", "NICKEL", "BRASS", "STEEL", "COPPER"));
	private List<String> regionList = new ArrayList<>(Arrays.asList("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"));


	public Query2(ExecutionEnvironment env, String sf) {
		this.env = env;
		this.sf = sf;
		
		//Read the tables and store it in datasets
		DataSet<Tuple5<Integer, String, String, String, Integer>> PartTbl = readPart();
		DataSet<Tuple7<Integer, String, String, Integer, String, Double, String>> SupplierTbl = readSupplier();
		DataSet<Tuple3<Integer, Integer, Double>> PartSuppTbl = readPartSupp();
		DataSet<Tuple3<Integer, String, Integer>> NationTbl = readNation();
		DataSet<Tuple2<Integer, String>> RegionTbl = readRegion();
		
		// Filter parrtTbl for the given random type and size
		String type = getRandomItem(typeList);
		int size = getRandomSize();
		
		System.out.println(" Part filtered for type" + type + " and size " + size);
		PartTbl = PartTbl.filter(partRecord -> (partRecord.f3.contains(type)) && (partRecord.f4.equals(size)));
		
		// Filter RegionTbl for the given random region
		String region = getRandomItem(regionList);
		RegionTbl = RegionTbl.filter(regionRecord -> regionRecord.f1.equals(region));
		
		// Join Part and PartSupp, to get the supplier and cost information
		// for the asked parts
		
		DataSet<Tuple4<Integer, String, Integer, Double>> partsWithSupplyDetail =
				PartTbl.join(PartSuppTbl).where(0).equalTo(0).projectFirst(0,2).projectSecond(1,2);
		
		
		DataSet<Tuple9<Integer, String, Double, String, String, Integer, String, Double, String>> 
				partsWithSupplierDetail =
				partsWithSupplyDetail.join(SupplierTbl).where(2).equalTo(0)
				.projectFirst(0,1,3).projectSecond(1,2,3,4,5,6);
		
		DataSet<Tuple10<Integer, String, Double, String, String, String, Double, String, String, Integer>> 
		partSupplierNation =
				partsWithSupplierDetail.join(NationTbl).where(5).equalTo(0)
		.projectFirst(0,1,2,3,4,6,7,8).projectSecond(1,2);
		
		DataSet<Tuple9<Integer, String, Double, String, String, Integer, String, Double, String>> 
		PSNationAndRegion =
				partSupplierNation.join(RegionTbl).where(9).equalTo(0)
		.projectFirst(0,1,2,3,4,5,6,7,8);
		
		//Find min cost
		DataSet<Tuple9<Integer, String, Double, String, String, Integer, String, Double, String>> 
		minCost = PSNationAndRegion.minBy(2);		
		
		// FInal result (p_partkey, p_mfgr, s_name, s_add, s_phone, s_acctble, s_comment, n_name)

		DataSet<Tuple> 
		finalResult =
				PSNationAndRegion.join(minCost).where(2).equalTo(2)
		.projectFirst(0,1,3,4,5,6,7,8).sortPartition(4, Order.DESCENDING).sortPartition(7, Order.ASCENDING)
		.sortPartition(2, Order.ASCENDING).sortPartition(0, Order.ASCENDING);
		
		try {
			System.out.println("The result of the query is: ");
			finalResult.print();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/* 
	 * The following columns are needed from part table 
	 * Part(p_partkey(int), p_name(String), p_mfgr(String), p_type(String),  p_size(int))
	 */

	private DataSet<Tuple5<Integer, String, String, String, Integer>> readPart() {
		CsvReader source = null;
		switch (sf) {
		case ("1.0"):
			source = env.readCsvFile(PathConfig.PART_1);
			break;
		default:
			source = env.readCsvFile(PathConfig.PART_1);
			break;
		}

		return source.fieldDelimiter("|").includeFields("111011000").types(Integer.class, String.class, String.class, String.class, Integer.class);
	}
	
	
	/* 
	 * The following columns are needed from supplier table
	 * SUPPLIER(s_suppkey(int), s_name(String), s_address(String), s_nationkey(int), s_phone(String), 
	 * s_acctbal(double), s_comment(String)); 
	 */
	
	private DataSet<Tuple7<Integer, String, String, Integer, String, Double, String>> readSupplier() {
		CsvReader source = null;
		switch (sf) {
		case ("1.0"):
			source = env.readCsvFile(PathConfig.SUPPLIER_1);
			break;
		default:
			source = env.readCsvFile(PathConfig.SUPPLIER_1);
			break;
		}

		return source.fieldDelimiter("|").includeFields("1111111").types(Integer.class, String.class, String.class, Integer.class, 
				String.class, Double.class, String.class);
	}
	
	/* 
	 * The following columns are needed from partsupp table
	 * Partsupp(ps_partkey(int), ps_suppkey(int), ps_suplycost(Double))
	 */
	
	private DataSet<Tuple3<Integer, Integer, Double>> readPartSupp() {
		CsvReader source = null;
		switch (sf) {
		case ("1.0"):
			source = env.readCsvFile(PathConfig.PARTSUPP_1);
			break;
		default:
			source = env.readCsvFile(PathConfig.PARTSUPP_1);
			break;
		}

		return source.fieldDelimiter("|").includeFields("11010").types(Integer.class, Integer.class, Double.class);
	}
	
	/* 
	 * The following columns are needed from Nation table
	 * Nation(n_nationkey(int), n_name(String), n_regionkey(int))
	 */
	private DataSet<Tuple3<Integer, String, Integer>> readNation() {
		CsvReader source = null;
		switch (sf) {
		case ("1.0"):
			source = env.readCsvFile(PathConfig.NATION_1);
			break;
		default:
			source = env.readCsvFile(PathConfig.NATION_1);
			break;
		}

		return source.fieldDelimiter("|").includeFields("1110").types(Integer.class, String.class, Integer.class);
	}
	
	
	/* 
	 * The following columns are needed from Region table
	 * Region(r_regionkey(int), r_name(int))
	 */
	private DataSet<Tuple2<Integer, String>> readRegion() {
		CsvReader source = null;
		switch (sf) {
		case ("1.0"):
			source = env.readCsvFile(PathConfig.Region_1);
			break;
		default:
			source = env.readCsvFile(PathConfig.Region_1);
			break;
		}

		return source.fieldDelimiter("|").includeFields("110").types(Integer.class, String.class);
	}
	
	// get a random item from a list of strings.
	private String getRandomItem(List<String> list){
		Random randomizer = new Random();
		String random = list.get(randomizer.nextInt(list.size()));
		return random;
	}
	
	//get random integer for size
	private int getRandomSize() {
		return ThreadLocalRandom.current().nextInt(1, 50 + 1);
	}
}