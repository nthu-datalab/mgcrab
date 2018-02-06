package netdb.software.benchmark.tpcc.rte;

import netdb.software.benchmark.tpcc.TpccBenchmarker;
import netdb.software.benchmark.tpcc.TpccStatisticMgr;
import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.remote.SutConnection;

public abstract class RemoteTerminalEmulator extends Thread {

	private static Integer rteCount = 1;

	protected Object connArgs[];

	private volatile boolean stopBenchmark;
	private volatile boolean isWarmingUp = true;

	protected SutConnection conn;

	public void stopBenchmark() {
		stopBenchmark = true;
	}

	public RemoteTerminalEmulator(Object[] connArgs) {
		this.connArgs = connArgs;
	}

	@Override
	public void run() {
		synchronized (rteCount) {
			setName("tpcc-rte-" + rteCount);
			rteCount++;
		}

		conn = TpccBenchmarker.getConnection(connArgs);
		// TODO refactor statMgr
		TpccStatisticMgr statMgr = TpccBenchmarker.statMgr;

		while (!stopBenchmark) {
			TxnResultSet rs = executeTxnCycle();
//			if (!isWarmingUp)
				statMgr.processTxnResult(rs);
		}
	}

	public void startRecordStatistic() {
		isWarmingUp = false;
	}

	protected abstract TxnResultSet executeTxnCycle();

	// private TxnResultSet executeTxnCycle() {
	// TransactionType type = TxnMixGenerator.nextTransactionType();
	// TpccTxnExecutor txn = executorFactory.getTxnExecutor(type, conn,
	// homeWid);
	// return txn.execute();
	// }

}