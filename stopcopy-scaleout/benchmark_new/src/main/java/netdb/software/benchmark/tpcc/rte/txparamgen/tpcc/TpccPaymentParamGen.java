package netdb.software.benchmark.tpcc.rte.txparamgen.tpcc;

import static netdb.software.benchmark.tpcc.TransactionType.PAYMENT;
import netdb.software.benchmark.tpcc.TpccBenchmarker;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;

public class TpccPaymentParamGen extends TpccTxParamGenerator {

	private int wid;

	public TpccPaymentParamGen(int homeWarehouseId) {
		this.wid = homeWarehouseId;
	}

	@Override
	public TransactionType getTxnType() {
		return PAYMENT;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_PAYMENT * 1000;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rg = TpccBenchmarker.generator;
		// pars = {wid, did, cwid, cdid, cid/clast, hAmount}
		Object[] pars = new Object[6];
		pars[0] = wid;
		pars[1] = rg.number(1, 10);
		/*
		 * Customer resident warehouse is the home warehouse 85% of the time and
		 * is a randomly selected remote warehouse 15% of the time.
		 */
		if (rg.rng().nextDouble() >= 0.85 && TpccConstants.NUM_WAREHOUSES > 1) {
			pars[2] = rg.numberExcluding(1, TpccConstants.NUM_WAREHOUSES, wid);
			pars[3] = rg.number(1, 10);
		} else {
			pars[2] = wid;
			pars[3] = pars[1];
		}

		/*
		 * The customer is randomly selected 60% of the time by last name and
		 * 40% of time by id.
		 */
//		if (rg.rng().nextDouble() >= 0.60)
//			pars[4] = rg.makeRandomLastName(false);
//		else
			pars[4] = rg.NURand(RandomValueGenerator.NU_CID, 1,
					TpccConstants.CUSTOMERS_PER_DISTRICT);
		pars[5] = rg.fixedDecimalNumber(2, 1.00, 5000.00);
		return pars;
	}

	@Override
	public long getThinkTime() {
		double r = TpccBenchmarker.generator.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_PAYMENT * 1000;
	}

}
