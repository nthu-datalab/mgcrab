package netdb.software.benchmark.tpcc.rte;

import static netdb.software.benchmark.tpcc.TransactionType.ORDER_STATUS;
import netdb.software.benchmark.tpcc.TpccBenchmarker;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;

public class OrderStatusTxnExecutor extends TpccTxnExecutor {
	private SutConnection spc;
	private int cwid;
	private Object[] pars;

	public OrderStatusTxnExecutor() {

	}

	public OrderStatusTxnExecutor(SutConnection spc, int homeWarehouseId) {
		this.cwid = homeWarehouseId;
		this.spc = spc;
	}

	@Override
	public void reinitialize(SutConnection conn, int wid) {
		pars = null;
		spc = conn;
		cwid = wid;
	}

	@Override
	public TransactionType getTxnType() {
		return ORDER_STATUS;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_ORDER_STATUS * 1000;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rg = TpccBenchmarker.generator;

		// pars = {cwid, cdid, cid/clast}
		pars = new Object[3];
		pars[0] = cwid;
		pars[1] = rg.number(1, 10);
		/*
		 * The customer is randomly selected 60% of the time by last name and
		 * 40% of time by id.
		 */
		// if (rg.rng().nextDouble() >= 0.60)
		// pars[2] = rg.makeRandomLastName(false);
		// else
		pars[2] = rg.NURand(RandomValueGenerator.NU_CID, 1,
				TpccConstants.CUSTOMERS_PER_DISTRICT);
		return pars;
	}

	@Override
	public SutResultSet callStoredProc() {
		try {
			SutResultSet result = spc.callStoredProc(ORDER_STATUS.ordinal(),
					pars);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public long getThinkTime() {
		double r = TpccBenchmarker.generator.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_ORDER_STATUS
				* 1000;
	}
}
