package netdb.software.benchmark.tpce;

import static netdb.software.benchmark.tpce.TransactionType.START_PROFILING;
import static netdb.software.benchmark.tpce.TransactionType.STOP_PROFILING;

import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.App;
import netdb.software.benchmark.Benchmark;
import netdb.software.benchmark.WorkloadOperator;
import netdb.software.benchmark.tpce.remote.SutConnection;
import netdb.software.benchmark.tpce.remote.SutDriver;
import netdb.software.benchmark.tpce.remote.vanilladb.VanillaDbDriver;
import netdb.software.benchmark.tpce.remote.vanilladddb.VanillaDdDbDriver;

public class TpceBenchmark extends Benchmark {
	public static TpceStatisticMgr statMgr;
	private static Logger logger = Logger.getLogger(TpceBenchmark.class
			.getName());

	public final static int CE_COUNT = TestingParameters.NUM_RTES;

	public TpceBenchmark() {
	}

	@Override
	public void initialize() {
		workload = new TpceWorkload();
		statMgr = new TpceStatisticMgr();
		for (int i = 0; i < CE_COUNT; i++)
			operators.add(new TpceWorkloadOperator(i, workload));
	}

	@Override
	public void load() {
		if (logger.isLoggable(Level.INFO))
			logger.info("loading the testbed of tpce benchmark...");
		try {
			SutDriver sd = getDriver();
			// hard code the node id for group comm. of testbed loader process
			SutConnection spc = sd.connectToSut(0);
			spc.callStoredProc(TransactionType.SCHEMA_BUILDER.ordinal());
			spc.callStoredProc(TransactionType.TESTBED_LOADER.ordinal());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void fullTableScan() {
		// TODO

	}

	@Override
	public void start() {
		if (logger.isLoggable(Level.INFO))
			logger.info("starting benchmark...");

		for (WorkloadOperator op : operators)
			op.start();

		try {

			// after warm up
			Thread.sleep(TestingParameters.WARM_UP_INTERVAL);

			if (App.PROFILE)
				startProfiling();

			for (WorkloadOperator op : operators)
				((TpceWorkloadOperator) op).startRecordStatistic();

			// benchmark finished
			Thread.sleep(TestingParameters.BENCHMARK_INTERVAL);
			for (WorkloadOperator op : operators)
				((TpceWorkloadOperator) op).stopOperator();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (App.PROFILE)
			stopProfiling();

		if (logger.isLoggable(Level.INFO))
			logger.info("Tpcc benchmark finished...");

	}

	@Override
	public void report() {
		statMgr.outputReport();
	}

	public void startProfiling() {
		if (App.myNodeId == 0) {
			if (logger.isLoggable(Level.INFO))
				logger.info("start profiler and collecting profiling data on server side...");
			try {
				SutDriver sd = getDriver();
				// hard code the node id for group comm. of testbed loader
				// process
				SutConnection spc = sd.connectToSut(0);
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
				SutDriver sd = getDriver();
				// hard code the node id for group comm. of testbed loader
				// process
				SutConnection spc = sd.connectToSut(0);
				spc.callStoredProc(STOP_PROFILING.ordinal());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private SutDriver getDriver() {
		SutDriver driver;
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
		return driver;
	}
}