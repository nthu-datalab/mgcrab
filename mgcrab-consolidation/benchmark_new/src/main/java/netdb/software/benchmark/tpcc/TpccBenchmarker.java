package netdb.software.benchmark.tpcc;

import static netdb.software.benchmark.tpcc.TransactionType.FULL_TABLE_SCAN;
import static netdb.software.benchmark.tpcc.TransactionType.START_MIGRATION;
import static netdb.software.benchmark.tpcc.TransactionType.START_PROFILING;
import static netdb.software.benchmark.tpcc.TransactionType.STOP_PROFILING;

import java.sql.SQLException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutDriver;
import netdb.software.benchmark.tpcc.remote.vanilladb.VanillaDbDriver;
import netdb.software.benchmark.tpcc.remote.vanilladddb.VanillaDdDbDriver;
import netdb.software.benchmark.tpcc.rte.RemoteTerminalEmulator;
import netdb.software.benchmark.tpcc.rte.tpcc.MicroBenchmarkRte;
import netdb.software.benchmark.tpcc.rte.tpcc.TpccRte;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;
import netdb.software.benchmark.tpcc.util.YcsbLatestGenerator;

public class TpccBenchmarker {
	private static Logger logger = Logger.getLogger(TpccBenchmarker.class
			.getName());

	public static RandomValueGenerator generator;
	public static TpccStatisticMgr statMgr;
	public static SutDriver driver;
	
	private YcsbLatestGenerator rteMainPartitionRandom = new YcsbLatestGenerator(3, 0.9);
	private Random uniformRandom = new Random();

	public TpccBenchmarker() {
		statMgr = new TpccStatisticMgr();
		generator = new RandomValueGenerator();
		initDriver();
	}

	public void load() {
		if (logger.isLoggable(Level.INFO))
			logger.info("loading the testbed of tpcc benchmark...");
		try {
			SutConnection spc = TpccBenchmarker.getConnection(0);
			spc.callStoredProc(TransactionType.SCHEMA_BUILDER.ordinal());
			spc.callStoredProc(TransactionType.TESTBED_LOADER.ordinal());
			// spc.callStoredProc(MYSQL_TESTBED_LOADER.pid());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void fullTableScan() {
		if (logger.isLoggable(Level.INFO))
			logger.info("preload the data to memory...");
		try {
			SutConnection spc = TpccBenchmarker.getConnection(0);
			spc.callStoredProc(FULL_TABLE_SCAN.ordinal());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		if (logger.isLoggable(Level.INFO))
			logger.info("running tpcc benchmark...");

		RemoteTerminalEmulator[] emulators;
		
		// XXX: For consolidation experiments (5:1:1)
		if (App.myNodeId == 0)
			emulators = new RemoteTerminalEmulator[50];
		else
			emulators = new RemoteTerminalEmulator[10];
		
		for (int i = 0; i < emulators.length; i++) {
			emulators[i] = getRte(0, i);
		}

		try {
			Thread.sleep(1500);
			for (int i = 0; i < emulators.length; i++) {
				emulators[i].start();
				System.out.println("RTE start: " + i);
			}

			// warm up finished
			Thread.sleep(TestingParameters.WARM_UP_INTERVAL);
			
			// start migration
			startMigration();

			if (App.PROFILE)
				startProfiling();
			for (int i = 0; i < emulators.length; i++)
				emulators[i].startRecordStatistic();
			// benchmark finished
			Thread.sleep(TestingParameters.BENCHMARK_INTERVAL);
			for (int i = 0; i < emulators.length; i++)
				emulators[i].stopBenchmark();

			if (App.PROFILE)
				stopProfiling();

			// for (int i = 0; i < emulators.length; i++)
			// emulators[i].join();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Tpcc benchmark finished...");
	}

	public void report() {
		statMgr.outputReport();
	}
	
	public void startMigration() {
		if (App.myNodeId == 0) {
			if (logger.isLoggable(Level.INFO))
				logger.info("start migration at: " + System.currentTimeMillis());
			try {
				SutConnection spc = TpccBenchmarker.getConnection(0);
				spc.callStoredProc(TransactionType.MIGRATION_ANALYSIS.ordinal());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void startProfiling() {
		if (App.myNodeId == 0) {
			if (logger.isLoggable(Level.INFO))
				logger.info("start profiler and collecting profiling data on server side...");
			try {
				SutConnection spc = TpccBenchmarker.getConnection(0);
				spc.callStoredProc(START_PROFILING.ordinal());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void stopProfiling() {
		if (App.myNodeId == 0) {
			if (logger.isLoggable(Level.INFO))
				logger.info("stop profiler and generate report...");
			try {
				SutConnection spc = TpccBenchmarker.getConnection(0);
				spc.callStoredProc(STOP_PROFILING.ordinal());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void initDriver() {
		switch (TestingParameters.SUT) {
		case TestingParameters.SUT_VANILLA_DB:
			driver = new VanillaDbDriver();
			break;
		case TestingParameters.SUT_VANILLA_DDDB:
			driver = new VanillaDdDbDriver();
			break;
		default:
			throw new UnsupportedOperationException("wrong sut id");
		}
	}

	public static SutConnection getConnection(Object... args) {
		try {
			return driver.connectToSut(args);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static int counter = 0;

	private RemoteTerminalEmulator getRte(int homeWid, Object... args) {
		RemoteTerminalEmulator rte;
		// TODO switch benchmark type here
		if (TestingParameters.IS_MICROBENCHMARK) {
//			rte = new MicroBenchmarkRte((int) rteMainPartitionRandom.nextValue() - 1, args);
			
			// 5:1:5
//			if (counter <= 4) {
//				rte = new MicroBenchmarkRte(0, args);
//			} else if (counter <= 5) {
//				rte = new MicroBenchmarkRte(1, args);
//			} else {
//				rte = new MicroBenchmarkRte(2, args);
//			}
//			
//			counter = (counter + 1) % 11;
			
			// 2:1:1
//			if (counter <= 1) {
//				rte = new MicroBenchmarkRte(0, args);
//			} else if (counter <= 2) {
//				rte = new MicroBenchmarkRte(1, args);
//			} else {
//				rte = new MicroBenchmarkRte(2, args);
//			}
//			counter = (counter + 1) % 4;
			
			// 5:1:1
//			if (counter <= 4) {
//				rte = new MicroBenchmarkRte(0, args);
//			} else if (counter <= 5) {
//				rte = new MicroBenchmarkRte(1, args);
//			} else {
//				rte = new MicroBenchmarkRte(2, args);
//			}
//			counter = (counter + 1) % 7;
			
			// 3:1:2
//			if (counter <= 2) {
//				rte = new MicroBenchmarkRte(0, args);
//			} else if (counter <= 3) {
//				rte = new MicroBenchmarkRte(1, args);
//			} else {
//				rte = new MicroBenchmarkRte(2, args);
//			}
//			counter = (counter + 1) % 6;
			
//			rte = new MicroBenchmarkRte(counter, args);
//			counter = (counter + 1) % 3;
			
			// XXX: For consolidation experiments
			rte = new MicroBenchmarkRte(App.myNodeId, args);
		} else
			rte = new TpccRte(homeWid, args);
		return rte;
	}
}