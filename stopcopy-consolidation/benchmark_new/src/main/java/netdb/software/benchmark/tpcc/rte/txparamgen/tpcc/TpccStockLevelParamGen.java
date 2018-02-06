package netdb.software.benchmark.tpcc.rte.txparamgen.tpcc;

import static netdb.software.benchmark.tpcc.TransactionType.STOCK_LEVEL;
import netdb.software.benchmark.tpcc.TpccBenchmarker;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;

public class TpccStockLevelParamGen extends TpccTxParamGenerator {

	private int wid;

	public TpccStockLevelParamGen(int homeWarehouseId) {
		this.wid = homeWarehouseId;
	}

	@Override
	public TransactionType getTxnType() {
		return STOCK_LEVEL;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rg = TpccBenchmarker.generator;
		// pars = {wid, did, threshold}
		Object[] pars = new Object[3];
		pars[0] = wid;
		pars[1] = rg.number(1, 10);
		pars[2] = rg.number(10, 20);
		return pars;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_STOCK_LEVEL * 1000;
	}

	@Override
	public long getThinkTime() {
		double r = TpccBenchmarker.generator.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_STOCK_LEVEL * 1000;
	}

}
