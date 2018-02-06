package netdb.software.benchmark.tpcc.rte;

import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;

public abstract class TpccTxnExecutor {

	protected final static boolean ENABLE_THINK_AND_KEYING_TIME;
	static {
		String prop = System.getProperty(TpccTxnExecutor.class.getName()
				+ ".ENABLE_THINK_AND_KEYING_TIME");
		ENABLE_THINK_AND_KEYING_TIME = (prop == null) ? false : Boolean
				.parseBoolean(prop.trim());
	}

	public static TpccTxnExecutor getTxnExecutor(TransactionType type,
			SutConnection conn, int wid) {
		switch (type) {
		case NEW_ORDER:
			return new NewOrderTxnExecutor(conn, wid);
		case PAYMENT:
			return new PaymentTxnExecutor(conn, wid);
		case ORDER_STATUS:
			return new OrderStatusTxnExecutor(conn, wid);
		case DELIVERY:
			return new DeliveryTxnExecutor(conn, wid);
		case STOCK_LEVEL:
			return new StockLevelTxnExecutor(conn, wid);
		default:
			throw new IllegalArgumentException();
		}
	}

	public abstract TransactionType getTxnType();

	public abstract long getKeyingTime();

	public abstract Object[] generateParameter();

	public abstract SutResultSet callStoredProc();

	public abstract void reinitialize(SutConnection conn, int wid);

	/**
	 * Return think time for this transaction type. This is defined in 5.2.5.4
	 * session in TPC-C 5.11 document.
	 */
	public abstract long getThinkTime();

	public TxnResultSet execute() {
		try {
			TxnResultSet rs = new TxnResultSet();
			rs.setTxnType(getTxnType());

			// keying
			if (ENABLE_THINK_AND_KEYING_TIME) {
				// wait for a keying time and generate parameters
				long t = getKeyingTime();
				rs.setKeyingTime(t);
				Thread.sleep(t);
			} else
				rs.setKeyingTime(0);

			// generate parameters
			generateParameter();

			// send txn request and start measure txn response time
			long txnRT = System.nanoTime();
			SutResultSet result = callStoredProc();

			// measure txn response time
			txnRT = System.nanoTime() - txnRT;

			// display output
			System.out.println(getTxnType() + " " + result.outputMsg());

			rs.setTxnIsCommited(result.isCommitted());
			rs.setOutMsg(result.outputMsg());
			rs.setTxnResponseTimeNs(txnRT);

			// thinking
			if (ENABLE_THINK_AND_KEYING_TIME) {
				// wait for a think time
				long t = getThinkTime();
				Thread.sleep(t);
				rs.setThinkTime(t);
			} else
				rs.setThinkTime(0);

			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
}