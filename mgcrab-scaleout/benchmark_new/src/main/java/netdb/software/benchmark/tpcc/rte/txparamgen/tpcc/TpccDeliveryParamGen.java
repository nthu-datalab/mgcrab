package netdb.software.benchmark.tpcc.rte.txparamgen.tpcc;

import static netdb.software.benchmark.tpcc.TransactionType.DELIVERY;
import netdb.software.benchmark.tpcc.TpccBenchmarker;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;

public class TpccDeliveryParamGen extends TpccTxParamGenerator {

	private int wid;

	public TpccDeliveryParamGen(int homeWarehouseId) {
		this.wid = homeWarehouseId;
	}

	@Override
	public TransactionType getTxnType() {
		return DELIVERY;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rg = TpccBenchmarker.generator;
		Object[] pars = new Object[2];
		pars[0] = wid;
		pars[1] = rg.number(1, 10);
		return pars;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_DELIVERY * 1000;
	}

	@Override
	public long getThinkTime() {
		double r = TpccBenchmarker.generator.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_DELIVERY * 1000;
	}

}
