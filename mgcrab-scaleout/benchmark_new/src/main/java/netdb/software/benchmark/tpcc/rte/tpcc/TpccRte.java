package netdb.software.benchmark.tpcc.rte.tpcc;

import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.TxnMixGenerator;
import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.rte.RemoteTerminalEmulator;
import netdb.software.benchmark.tpcc.rte.executor.TpccTxExecutor;
import netdb.software.benchmark.tpcc.rte.txparamgen.tpcc.TpccTxParamGenerator;

public class TpccRte extends RemoteTerminalEmulator {

	private int homeWid;

	public TpccRte(int homeWid, Object[] connArgs) {
		super(connArgs);
		this.homeWid = homeWid;
	}

	@Override
	protected TxnResultSet executeTxnCycle() {
		TransactionType type = TxnMixGenerator.nextTransactionType();
		TpccTxExecutor tx = new TpccTxExecutor(
				TpccTxParamGenerator.getParamGen(type, homeWid));
		return tx.execute(conn);
	}

}
