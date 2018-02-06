package netdb.software.benchmark;

import netdb.software.benchmark.tpce.TxnResultSet;

public abstract class TxnExecutor {

	/**
	 * Execute this transaction in a defined workflow.
	 * 
	 * @return The transaction executing result.
	 */
	public abstract TxnResultSet execute();

}
