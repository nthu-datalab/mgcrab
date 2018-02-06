package netdb.software.benchmark.tpcc.procedure.vanilladb;

import netdb.software.benchmark.tpcc.TransactionType;

import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class TpccStoredProcFactory {

	public static StoredProcedure getStoredProcedure(int pid) {
		StoredProcedure sp;
		switch (TransactionType.values()[pid]) {
		case NEW_ORDER:
			sp = new NewOrderProc();
			break;
		case PAYMENT:
			sp = new PaymentProc();
			break;
		case ORDER_STATUS:
			sp = new OrderStatusProc();
			break;
		case DELIVERY:
			sp = new DeliveryProc();
			break;
		case STOCK_LEVEL:
			sp = new StockLevelProc();
			break;
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc();
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc();
			break;
		case FULL_TABLE_SCAN:
			sp = new FullTableScanProc();
			break;
		case START_PROFILING:
			sp = new StartProfilingProc();
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc();
			break;
		case MICRO_BENCHMARK:
			sp = new MicroBenchmarkProc();
			break;
		default:
			throw new UnsupportedOperationException();
		}
		return sp;
	}
}