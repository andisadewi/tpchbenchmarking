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
				new String[] { "orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount",
						"tax", "returnflag", "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct",
						"shipmode", "comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(),
			Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(), Types.STRING(), Types.STRING(),
			Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read supplier
		CsvTableSource supplier = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.SUPPLIER,
				new String[] { "suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
			Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read part
		CsvTableSource part = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.PART,
				new String[] { "partkey", "name", "mfgr", "brand", "type", "size", "container",
						"retailprice", "comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
			Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read partsupp
		CsvTableSource partsupp = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.PARTSUPP,
				new String[] { "partkey", "suppkey", "availqty", "supplycost", "comment" },
				new TypeInformation<?>[] { Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read customer
		CsvTableSource customer = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.CUSTOMER,
				new String[] { "custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment",
		"comment"},
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
			Types.DOUBLE(), Types.STRING(), Types.STRING()},
				"|", "\n", null, false, null, false);

		// read nation
		CsvTableSource nation = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.NATION,
				new String[] { "nationkey", "name", "regionkey", "comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.INT(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read region
		CsvTableSource region = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.REGION,
				new String[] { "regionkey", "name", "comment" },
				new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.STRING() },
				"|", "\n", null, false, null, false);

		// read orders
		CsvTableSource orders = new CsvTableSource(PathConfig.BASE_DIR + sf + "/" + PathConfig.ORDERS,
				new String[] { "orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk",
						"shipriority", "comment"},
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
