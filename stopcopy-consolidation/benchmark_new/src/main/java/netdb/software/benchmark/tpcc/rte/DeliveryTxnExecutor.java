package netdb.software.benchmark.tpcc.rte;

import static netdb.software.benchmark.tpcc.TransactionType.DELIVERY;
import netdb.software.benchmark.tpcc.TpccBenchmarker;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;

public class DeliveryTxnExecutor extends TpccTxnExecutor {
	private SutConnection spc;
	private int wid;
	private Object[] pars;

	public DeliveryTxnExecutor() {
	}

	public DeliveryTxnExecutor(SutConnection spc, int homeWarehouseId) {
		this.wid = homeWarehouseId;
		this.spc = spc;
	}

	@Override
	public void reinitialize(SutConnection conn, int wid) {
		pars = null;
		spc = conn;
		this.wid = wid;
	}

	@Override
	public TransactionType getTxnType() {
		return DELIVERY;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_DELIVERY * 1000;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rg = TpccBenchmarker.generator;
		pars = new Object[2];
		pars[0] = wid;
		pars[1] = rg.number(1, 10);
		return pars;
	}

	@Override
	public SutResultSet callStoredProc() {
		try {
			SutResultSet result = spc.callStoredProc(DELIVERY.ordinal(), pars);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public long getThinkTime() {
		double r = TpccBenchmarker.generator.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_DELIVERY * 1000;
	}
}
