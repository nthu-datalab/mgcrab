package netdb.software.benchmark.tpcc;

import java.util.LinkedList;

import netdb.software.benchmark.tpcc.vanilladddb.migration.TpccMigrationManager;

public class TestingParameters {
	public static final int SUT_VANILLA_DB = 1, SUT_VANILLA_DDDB = 2;

	public static final long BENCHMARK_INTERVAL;
	public static final long WARM_UP_INTERVAL;
	public static final int SUT;
	public static final int NUM_RTES;
	public static final int RTE_HOME_WAREHOUSE_IDS[];

	public static final boolean IS_MYSQL;
	public static final boolean IS_MICROBENCHMARK;

	static {
		String prop = System.getProperty(TestingParameters.class.getName()
				+ ".BENCHMARK_INTERVAL");
		BENCHMARK_INTERVAL = (prop == null ? 100000 : Integer.parseInt(prop
				.trim()));
		prop = System.getProperty(TestingParameters.class.getName()
				+ ".WARM_UP_INTERVAL");
		WARM_UP_INTERVAL = (prop == null ? 30000 : Integer
				.parseInt(prop.trim()));
		prop = System.getProperty(TestingParameters.class.getName() + ".SUT");
		SUT = (prop == null ? 1 : Integer.parseInt(prop.trim()));
//		prop = System.getProperty(TestingParameters.class.getName()
//				+ ".NUM_RTES");
//		NUM_RTES = (prop == null ? 1 : Integer.parseInt(prop.trim()));

//		RTE_HOME_WAREHOUSE_IDS = new int[NUM_RTES];
//		String[] rteHomeWarehouseIdStr = System.getProperty(
//				TestingParameters.class.getName() + ".RTE_HOME_WAREHOUSE_IDS")
//				.split(",");
//
//		if (rteHomeWarehouseIdStr.length != NUM_RTES)
//			throw new RuntimeException(
//					"the properties of rte information is not complete");
//		for (int i = 0; i < NUM_RTES; i++) {
//			RTE_HOME_WAREHOUSE_IDS[i] = Integer
//					.parseInt(rteHomeWarehouseIdStr[i].trim());
//		}

		prop = System.getProperty(TestingParameters.class.getName()
				+ ".IS_MICROBENCHMARK");
		IS_MICROBENCHMARK = (prop == null) ? false : Boolean.parseBoolean(prop
				.trim());
		prop = System.getProperty(TestingParameters.class.getName()
				+ ".IS_MYSQL");
		IS_MYSQL = (prop == null) ? false : Boolean.parseBoolean(prop.trim());
		
		int rteNum = 0;
		int[] rteIds = null;
		if (IS_MICROBENCHMARK) {
			// Micro-benchmarks
			prop = System.getProperty(TestingParameters.class.getName()
					+ ".NUM_RTES");
			rteNum = (prop == null ? 1 : Integer.parseInt(prop.trim()));
			rteIds = new int[rteNum];
		} else {
			// TPC-C
			LinkedList<Integer> wids = new LinkedList<Integer>();
			int rtePerWarehouse = 2, migrationMult = 10;
			for (int wid = 1; wid <= TpccConstants.NUM_WAREHOUSES; wid++) {
				int max = rtePerWarehouse;
				if (wid >= TpccMigrationManager.MIN_WID_IN_MIGRATION_RANGE)
					max *= migrationMult;
				
				for (int i = 0; i < max; i++) {
					wids.add(wid);
				}
			}
			
			rteNum = wids.size();
			rteIds = new int[rteNum];
			int i = 0;
			for (Integer wid : wids) {
				rteIds[i] = wid;
				i++;
			}
		}
		
		NUM_RTES = rteNum;
		RTE_HOME_WAREHOUSE_IDS = rteIds;
	}
}