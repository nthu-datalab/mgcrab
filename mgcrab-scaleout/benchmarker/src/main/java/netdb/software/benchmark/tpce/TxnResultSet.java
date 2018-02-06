package netdb.software.benchmark.tpce;

public class TxnResultSet {
	private TransactionType txnType;
	private long txnResponseTime;
	private boolean txnIsCommited;
	private String outMsg;

	public TransactionType getTxnType() {
		return txnType;
	}

	public void setTxnType(TransactionType txnType) {
		this.txnType = txnType;
	}

	public long getTxnResponseTime() {
		return txnResponseTime;
	}

	public void setTxnResponseTime(long txnResponseTime) {
		this.txnResponseTime = txnResponseTime;
	}

	public boolean isTxnIsCommited() {
		return txnIsCommited;
	}

	public void setTxnIsCommited(boolean txnIsCommited) {
		this.txnIsCommited = txnIsCommited;
	}

	public String getOutMsg() {
		return outMsg;
	}

	public void setOutMsg(String outMsg) {
		this.outMsg = outMsg;
	}
}
