package netdb.software.benchmark.tpcc.procedure.mysql;

import netdb.software.benchmark.tpcc.TransactionType;

import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class MysqlTpccStoredProcFactory {
	public static StoredProcedure getStoredProcedure(TransactionType type) {
		StoredProcedure sp = null;
		switch (type) {
		case NEW_ORDER:
			sp = new MysqlNewOrderProc();
			break;
		case PAYMENT:
			sp = new MysqlPaymentProc();
			break;
		case ORDER_STATUS:
			sp = new MysqlOrderStatusProc();
			break;
		case DELIVERY:
			sp = new MysqlDeliveryProc();
			break;
		case STOCK_LEVEL:
			sp = new MysqlStockLevelProc();
			break;
		case SCHEMA_BUILDER:
			sp = new MysqlSchemaBuilderProc();
			break;
		case TESTBED_LOADER:
			sp = new MysqlTestbedLoaderProc();
			break;
		case FULL_TABLE_SCAN:

			break;
		case START_PROFILING:

			break;
		case STOP_PROFILING:

			break;
		case MICRO_BENCHMARK:
			sp = new MysqlMicroBenchmarkProc();
			break;
		default:
			throw new UnsupportedOperationException();
		}
		return sp;
	}
}
