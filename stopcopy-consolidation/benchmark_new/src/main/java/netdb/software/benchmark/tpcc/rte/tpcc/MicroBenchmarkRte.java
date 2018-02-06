package netdb.software.benchmark.tpcc.rte.tpcc;

import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.rte.RemoteTerminalEmulator;
import netdb.software.benchmark.tpcc.rte.executor.MicrobenchmarkTxExecutor;
import netdb.software.benchmark.tpcc.rte.executor.TransactionExecutor;
import netdb.software.benchmark.tpcc.rte.txparamgen.TxParamGenerator;
import netdb.software.benchmark.tpcc.rte.txparamgen.YcsbParamGen;

public class MicroBenchmarkRte extends RemoteTerminalEmulator {

	private TxParamGenerator paramGem;

	public MicroBenchmarkRte(int mainPartition, Object[] connArgs) {
		super(connArgs);
		// XXX: There should be a Ycsb Rte for it
//		paramGem = new MicrobenchmarkParamGen();
		paramGem = new YcsbParamGen(mainPartition);
	}

	@Override
	protected TxnResultSet executeTxnCycle() {
		TransactionExecutor tx = new MicrobenchmarkTxExecutor(paramGem);
		return tx.execute(conn);
	}

}
