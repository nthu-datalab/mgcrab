package netdb.software.benchmark.tpcc.procedure.mysql;

import netdb.software.benchmark.tpcc.TestingParameters;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.procedure.NewOrderProcedure;
import netdb.software.benchmark.tpcc.rte.NewOrderTxnExecutor;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class MysqlRunnerProc extends NewOrderProcedure implements
		StoredProcedure {

	private static final double[] WAREHOUSE_DISTRIBUTION;
	RandomValueGenerator rg = new RandomValueGenerator();

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

	public MysqlRunnerProc() {

	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	public SpResultSet execute() {
		int txCnt = 0;
		long time = System.currentTimeMillis();
		while (System.currentTimeMillis() - time < TestingParameters.WARM_UP_INTERVAL) {
			StoredProcedure sp = new MysqlNewOrderProc();
			sp.prepare(createNewOrderParams());
			sp.execute();
		}

		time = System.currentTimeMillis();
		while (System.currentTimeMillis() - time < TestingParameters.BENCHMARK_INTERVAL) {
			StoredProcedure sp = new MysqlNewOrderProc();
			sp.prepare(createNewOrderParams());
			sp.execute();
			txCnt++;
		}

		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		Type txCntType = Type.INTEGER;
		sch.addField("status", statusType);
		sch.addField("tx count", txCntType);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		rec.setVal("tx count", new IntegerConstant(txCnt));

		return new SpResultSet(sch, rec);

	}

	private Object[] createNewOrderParams() {
		boolean allLocal = true;
		// pars = {wid, did, cid, olCount, items[15][3], allLocal}
		Object[] pars = new Object[50];
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
			if (rg.rng().nextDouble() < 0.05
					&& TpccConstants.NUM_WAREHOUSES > 1) {
				pars[++j] = rg.numberExcluding(1, TpccConstants.NUM_WAREHOUSES,
						wid);
				allLocal = false;
			} else
				pars[++j] = wid;

			// ol_quantity
			pars[++j] = rg.number(1, 10);
		}
		pars[49] = allLocal;

		return pars;
	}
}
