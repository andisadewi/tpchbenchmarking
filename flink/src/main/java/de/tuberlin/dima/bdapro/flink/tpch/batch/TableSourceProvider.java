package de.tuberlin.dima.bdapro.flink.tpch.batch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

import de.tuberlin.dima.bdapro.flink.tpch.PathConfig;

public class TableSourceProvider {

	public static BatchTableEnvironment loadData(final ExecutionEnvironment env, final String sf) {
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		// read lineitem
		CsvTableSource lineitem = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.LINEITEM,
				new String[] { "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", ""
						+ "l_quantity", "l_extendedprice", "l_discount",
						"l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", 
						"l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(),
			Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(), Types.STRING(), Types.STRING(),
			Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read supplier
		CsvTableSource supplier = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.SUPPLIER,
				new String[] { "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
			Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read part
		CsvTableSource part = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.PART,
				new String[] { "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container",
						"p_retailprice", "p_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
			Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read partsupp
		CsvTableSource partsupp = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.PARTSUPP,
				new String[] { "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read customer
		CsvTableSource customer = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.CUSTOMER,
				new String[] { "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", 
						"c_acctbal", "c_mktsegment", "c_comment"},
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
			Types.DOUBLE(), Types.STRING(), Types.STRING()},
				"|", "\n", null, false, null, false);

		// read nation
		CsvTableSource nation = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.NATION,
				new String[] { "n_nationkey", "n_name", "n_regionkey", "n_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.INT(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read region
		CsvTableSource region = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.REGION,
				new String[] { "r_regionkey", "r_name", "r_comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read orders
		CsvTableSource orders = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.ORDERS,
				new String[] { "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk",
						"o_shipriority", "o_comment"},
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
			Types.STRING(), Types.STRING(), Types.INT(), Types.STRING() },
				"|", "\n", null, false, null, false);

		tableEnv.registerTableSource("lineitem", lineitem);
		tableEnv.registerTableSource("supplier", supplier);
		tableEnv.registerTableSource("part", part);
		tableEnv.registerTableSource("partsupp", partsupp);
		tableEnv.registerTableSource("customer", customer);
		tableEnv.registerTableSource("nation", nation);
		tableEnv.registerTableSource("region", region);
		tableEnv.registerTableSource("orders", orders);

		return tableEnv;
	}

}
