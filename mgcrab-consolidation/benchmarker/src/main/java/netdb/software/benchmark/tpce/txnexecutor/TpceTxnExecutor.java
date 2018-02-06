package netdb.software.benchmark.tpce.txnexecutor;

import java.sql.SQLException;
import java.util.Date;

import netdb.software.benchmark.TxnExecutor;
import netdb.software.benchmark.tpce.TransactionType;
import netdb.software.benchmark.tpce.TxnResultSet;
import netdb.software.benchmark.tpce.remote.SutConnection;
import netdb.software.benchmark.tpce.remote.SutResultSet;

public abstract class TpceTxnExecutor extends TxnExecutor {

	protected SutConnection spc;

	public TpceTxnExecutor(SutConnection spc) {
		this.spc = spc;
	}

	// ////////////////////
	// abstract methods
	// ////////////////////

	/**
	 * Generate and return the parameters.
	 */
	public abstract Object[] generateParameter();

	public abstract TransactionType getTxnType();

	/**
	 * A callback method after the last bit of server response received.
	 */
	public void onResponseReceived() {

	}

	@Override
	public TxnResultSet execute() {

		TxnResultSet result = new TxnResultSet();
		result.setTxnType(getTxnType());

		// generate parameters
		Object[] params = generateParameter();

		// cal remote stored procedure
		long txnRT = System.nanoTime();
		SutResultSet sutResult = callStoredProc(params);
		txnRT = System.nanoTime() - txnRT;

		Date _date = new Date();
		// System.out.println(getTxnType() + " " + _date + " "+
		// sutResult.outputMsg());

		onResponseReceived();

		result.setTxnIsCommited(sutResult.isCommitted());
		result.setOutMsg(sutResult.outputMsg());
		result.setTxnResponseTime(txnRT);

		return result;
	}

	public SutResultSet callStoredProc(Object[] params) {
		SutResultSet result = null;
		try {
			result = spc.callStoredProc(getTxnType().ordinal(), params);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

}
