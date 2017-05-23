package de.tuberlin.dima.bdapro.tpch.queries;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import de.tuberlin.dima.bdapro.tpch.Config;

public class Query5 extends Query {

	public Query5(ExecutionEnvironment env, String sf) {
		super(env, sf);
		// TODO Auto-generated constructor stub
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.bdapro.tpch.queries.Query#execute()
	 * 
	 * the following fields are needed from each table 
	 * 
	 * Customer(c_custkey, c_nationkey)10010000 
	 * orders(o_orderkey, o_custkey, o_orderdate)110010000
	 * lineitem(l_orderkey, l_suppkey, l_extendedprice, l_discount)1010011000000000
	 * supplier(s_suppkey, s_nationkey)1001000
	 * nation(n_nationkey, n_name, n_regionkey) 1110
	 * region(r_regionkey, r_name)110
	 * 
	 */
	
	@Override
	public List<Tuple2<String, Double>> execute() {
		// TODO Auto-generated method stub
		
		final DataSet<Tuple2<Integer, Integer>> customers = readCustomers();
		final DataSet<Tuple3<Integer, Integer, String>> orders = readOrders();
		final DataSet<Tuple2<Integer, Integer>> suppliers = readSuppliers();
		final DataSet<Tuple4<Integer, Integer, Double, Double>> lineitems = readLineitem();
		final DataSet<Tuple3<Integer, String, Integer>> nations = readNations();
		final DataSet<Tuple2<Integer, Integer>> regions = readRegions();
		try{
			final List<Tuple2<String, Double>> out = null;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	//Read Customers 
	private DataSet<Tuple2<Integer, Integer>> readCustomers() {
	final CsvReader source = getCSVReader(Config.CUSTOMER);
	return source.fieldDelimiter("|").includeFields("10010000")
			.types(Integer.class, Integer.class);
	}
	
	//Read Orders 
	private DataSet<Tuple3<Integer, Integer, String>> readOrders() {
	final CsvReader source = getCSVReader(Config.ORDERS);
	return source.fieldDelimiter("|").includeFields("110010000")
			.types(Integer.class, Integer.class, String.class);
	}
		
	//Read lineitems 
	private DataSet<Tuple4<Integer, Integer, Double, Double>> readLineitem() {
	final CsvReader source = getCSVReader(Config.LINEITEM);
	return source.fieldDelimiter("|").includeFields("1010011000000000")
			.types(Integer.class, Integer.class, Double.class, Double.class);
	}
	
	//Read Suppliers 
	private DataSet<Tuple2<Integer, Integer>> readSuppliers() {
	final CsvReader source = getCSVReader(Config.SUPPLIER);
	return source.fieldDelimiter("|").includeFields("1001000")
			.types(Integer.class, Integer.class);
	}
	
	//Read Natios 
	private DataSet<Tuple3<Integer, String, Integer>> readNations() {
	final CsvReader source = getCSVReader(Config.NATION);
	return source.fieldDelimiter("|").includeFields("1110")
			.types(Integer.class, String.class, Integer.class);
	}
	
	//Read Regions 
	private DataSet<Tuple2<Integer, Integer>> readRegions() {
	final CsvReader source = getCSVReader(Config.REGION);
	return source.fieldDelimiter("|").includeFields("110")
			.types(Integer.class, Integer.class);
	}
}
