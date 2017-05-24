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

	public static final String REGION = "region.tbl";

	public static final String PART = "part.tbl";

	public static final String PARTSUPP = "partsupp.tbl";


	public enum Nation{
		ALGERIA("ALGERIA", "AFRICA"), 
		ARGENTINA("ARGENTINA", "AMERICA"), 
		BRAZIL("BRAZIL", "AMERICA"),
		CANADA("CANADA", "AMERICA"),
		EGYPT("EGYPT", "MIDDLE EAST"),
		ETHIOPIA("ETHIOPIA", "AFRICA"),
		FRANCE("FRANCE", "EUROPE"),
		GERMANY("GERMANY", "EUROPE"),
		INDIA("INDIA", "ASIA"),
		INDONESIA("INDONESIA", "ASIA"),
		IRAN("IRAN", "MIDDLE EAST"),
		IRAQ("IRAQ", "MIDDLE EAST"),
		JAPAN("JAPAN", "ASIA"),
		JORDAN("JORDAN", "MIDDLE EAST"),
		KENYA("KENYA", "AFRICA"),
		MOROCCO("MOROCCO", "AFRICA"),
		MOZAMBIQUE("MOZAMBIQUE", "AFRICA"),
		PERU("PERU", "AMERICA"),
		CHINA("CHINA", "ASIA"),
		ROMANIA("ROMANIA", "EUROPE"),
		SAUDI_ARABIA("SAUDI ARABIA", "MIDDLE EAST"),
		VIETNAM("VIETNAM", "ASIA"),
		RUSSIA("RUSSIA", "EUROPE"),
		UNITED_KINGDOM("UNITED KINGDOM", "EUROPE"),
		UNITED_STATES("UNITED STATES", "AMERICA");

		private String name;

		private String region;

		private Nation(final String value, final String region){
			this.name = value;
			this.region = region;
		}

		public String getName(){
			return this.name;
		}

		public String getRegion(){
			return this.region;
		}

		public static Nation getRandomNationAndRegion(){
			int random = new Random().nextInt(values().length);
			return values()[random];
		}

		public static String getRandomNation(){
			return getRandomNationAndRegion().getName();
		}

		public static String getRandomRegion(){
			return getRandomNationAndRegion().getRegion();
		}
	}

}
