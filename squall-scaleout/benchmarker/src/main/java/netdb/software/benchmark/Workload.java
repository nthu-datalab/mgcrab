package netdb.software.benchmark;

import netdb.software.benchmark.tpce.remote.SutConnection;

public abstract class Workload {
	/**
	 * Initialize the workload, including schema builder and testbed loader.
	 */
	public abstract void initialize();

	/**
	 * Generate another new transaction that follows this workload.
	 * 
	 * @return The new transaction.
	 */
	public abstract TxnExecutor nextTxn(SutConnection spc);
}
