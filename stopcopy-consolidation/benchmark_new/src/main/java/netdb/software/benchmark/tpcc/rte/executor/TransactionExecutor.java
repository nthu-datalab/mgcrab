package netdb.software.benchmark.tpcc.rte.executor;

import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;
import netdb.software.benchmark.tpcc.rte.txparamgen.TxParamGenerator;

public abstract class TransactionExecutor {

	protected TxParamGenerator pg;

	public abstract TxnResultSet execute(SutConnection conn);

	protected SutResultSet callStoredProc(SutConnection spc, Object[] pars) {
		try {
			SutResultSet result = spc.callStoredProc(pg.getTxnType().ordinal(),
					pars);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

}
