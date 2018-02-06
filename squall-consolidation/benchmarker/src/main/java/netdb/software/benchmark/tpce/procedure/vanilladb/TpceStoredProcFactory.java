package netdb.software.benchmark.tpce.procedure.vanilladb;

import netdb.software.benchmark.tpce.TransactionType;

import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class TpceStoredProcFactory {

	public static StoredProcedure getStoredProcedure(int pid) {
		StoredProcedure sp = null;
		TransactionType type = TransactionType.values()[pid];
		System.out.println("pid:" + pid);
		switch (type) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc();
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc();
			break;
		case TRADE_ORDER:
			sp = new TradeOrderProc();
			break;
		default:
			throw new UnsupportedOperationException();
		}
		return sp;
	}
}