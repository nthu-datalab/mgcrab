package netdb.software.benchmark.tpcc.rte.txparamgen;

import netdb.software.benchmark.tpcc.TransactionType;

public interface TxParamGenerator {

	TransactionType getTxnType();

	Object[] generateParameter();

}
