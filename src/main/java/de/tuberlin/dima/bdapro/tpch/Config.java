package de.tuberlin.dima.bdapro.tpch;

import java.util.Random;

public class Config {

	public static final String BASE_DIR = System.getProperty("user.dir") + "/tpch2.17.2/dbgen/testdata/";

	////// TABLE NAMES //////
	public static final String LINEITEM = "lineitem.tbl";

	public static final String NATION = "nation.tbl";

	public static final String CUSTOMER = "customer.tbl";

	public static final String ORDERS = "orders.tbl";

	public static final String SUPPLIER = "supplier.tbl";
	
	
	public enum Nation{
		ALGERIA("ALGERIA"), 
		ARGENTINA("ARGENTINA"), 
		BRAZIL("BRAZIL"),
		CANADA("CANADA"),
		EGYPT("EGYPT"),
		ETHIOPIA("ETHIOPIA"),
		FRANCE("FRANCE"),
		GERMANY("GERMANY"),
		INDIA("INDIA"),
		INDONESIA("INDONESIA"),
		IRAN("IRAN"),
		IRAQ("IRAQ"),
		JAPAN("JAPAN"),
		JORDAN("JORDAN"),
		KENYA("KENYA"),
		MOROCCO("MOROCCO"),
		MOZAMBIQUE("MOZAMBIQUE"),
		PERU("PERU"),
		CHINA("CHINA"),
		ROMANIA("ROMANIA"),
		SAUDI_ARABIA("SAUDI ARABIA"),
		VIETNAM("VIETNAM"),
		RUSSIA("RUSSIA"),
		UNITED_KINGDOM("UNITED KINGDOM"),
		UNITED_STATES("UNITED STATES");
		
		private String name;
		
		private Nation(String value){
			this.name = value;
		}
		
		public String getName(){
			return this.name;
		}
		
		public static String getRandomNation(){
			int random = new Random().nextInt(values().length);
			return values()[random].getName();
		}
	}
}
