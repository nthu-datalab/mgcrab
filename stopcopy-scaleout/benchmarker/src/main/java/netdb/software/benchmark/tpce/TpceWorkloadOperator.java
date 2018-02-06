package netdb.software.benchmark.tpce;

import java.sql.SQLException;

import netdb.software.benchmark.TxnExecutor;
import netdb.software.benchmark.Workload;
import netdb.software.benchmark.WorkloadOperator;
import netdb.software.benchmark.tpce.remote.SutConnection;
import netdb.software.benchmark.tpce.remote.SutDriver;
import netdb.software.benchmark.tpce.remote.vanilladb.VanillaDbDriver;
import netdb.software.benchmark.tpce.remote.vanilladddb.VanillaDdDbDriver;

public class TpceWorkloadOperator extends WorkloadOperator {
	private int rteId;
	private volatile boolean stopBenchmark;
	private volatile boolean isWarmingUp = true;
	private SutConnection conn;

	public TpceWorkloadOperator(int rteId, Workload workload) {
		super(workload);
		this.rteId = rteId;
	}

	@Override
	public void run() {
		connectToSut();

		while (!stopBenchmark) {
			TxnExecutor executor = workload.nextTxn(conn);
			TxnResultSet rs = executor.execute();
			if (!isWarmingUp)
				TpceBenchmark.statMgr.processTxnResult(rs);
		}
	}

	public void connectToSut() {
		try {
			SutDriver cd = getDriver();
			conn = cd.connectToSut(rteId);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("cannot connect to SUT");
		}
	}

	public void stopOperator() {
		stopBenchmark = true;
	}

	public void startRecordStatistic() {
		isWarmingUp = false;
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
