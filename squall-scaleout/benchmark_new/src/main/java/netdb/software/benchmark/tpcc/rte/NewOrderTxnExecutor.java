package netdb.software.benchmark.tpcc.rte;

import static netdb.software.benchmark.tpcc.TransactionType.NEW_ORDER;
import netdb.software.benchmark.tpcc.TpccBenchmarker;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;

public class NewOrderTxnExecutor extends TpccTxnExecutor {
	private SutConnection spc;
	private int wid;
	private Object[] pars;

	private static final double[] WAREHOUSE_DISTRIBUTION;

	static {
		String[] wd = System
				.getProperty(
						NewOrderTxnExecutor.class.getName()
								+ ".WAREHOUSE_DISTRIBUTION").split(",");

		if (wd.length != TpccConstants.NUM_WAREHOUSES)
			throw new RuntimeException(
					"the warehouse information is not complete");

		WAREHOUSE_DISTRIBUTION = new double[TpccConstants.NUM_WAREHOUSES];
		for (int i = 0; i < TpccConstants.NUM_WAREHOUSES; i++)
			WAREHOUSE_DISTRIBUTION[i] = Double.parseDouble(wd[i].trim());
	}

	public NewOrderTxnExecutor() {
	}

	public NewOrderTxnExecutor(SutConnection spc, int homeWarehouseId) {
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
		return NEW_ORDER;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_NEW_ORDER * 1000;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rg = TpccBenchmarker.generator;

		/*
		 * The return value of RandomChooseFromDistribution Method start from 1.
		 */
		// if (RemoteTerminalEmulator.IS_BATCH_REQUEST)
		// wid = rg.randomChooseFromDistribution(WAREHOUSE_DISTRIBUTION);

		boolean allLocal = true;
		// pars = {wid, did, cid, olCount, items[15][3], allLocal}
		pars = new Object[50];
		pars[0] = wid;
		pars[1] = rg.number(1, 10);
		pars[2] = rg.NURand(RandomValueGenerator.NU_CID, 1,
				TpccConstants.CUSTOMERS_PER_DISTRICT);
		int olCount = rg.number(5, 15);
		pars[3] = olCount;

		for (int i = 0; i < olCount; i++) {
			int j = 4 + i * 3;
			/*
			 * ol_i_id. 1% of the New-Order txs are chosen at random to simulate
			 * user data entry errors
			 */
			// if (rg.rng().nextDouble() < 0.01)
			// pars[j] = TpccConstants.NUM_ITEMS + 15; // choose unused item id
			// else
			pars[j] = rg.NURand(RandomValueGenerator.NU_OLIID, 1,
					TpccConstants.NUM_ITEMS);

			// ol_supply_w_id. 1% of items are supplied by remote warehouse
			// if (rg.rng().nextDouble() < 0.05
			// && TpccConstants.NUM_WAREHOUSES > 1) {
			// pars[++j] = rg.numberExcluding(1, TpccConstants.NUM_WAREHOUSES,
			// wid);
			// allLocal = false;
			// } else
			pars[++j] = wid;

			// ol_quantity
			pars[++j] = rg.number(1, 10);
		}
		pars[49] = allLocal;

		return pars;
	}

	@Override
	public SutResultSet callStoredProc() {
		try {
			SutResultSet result = spc.callStoredProc(NEW_ORDER.ordinal(), pars);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public long getThinkTime() {
		double r = TpccBenchmarker.generator.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_NEW_ORDER * 1000;
	}
}
