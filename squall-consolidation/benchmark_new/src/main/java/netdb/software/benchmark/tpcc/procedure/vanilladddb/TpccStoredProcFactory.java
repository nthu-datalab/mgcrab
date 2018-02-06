package netdb.software.benchmark.tpcc.procedure.vanilladddb;

import netdb.software.benchmark.tpcc.TransactionType;

import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.DdStoredProcedureFactory;

public class TpccStoredProcFactory extends DdStoredProcedureFactory {

	@Override
	public DdStoredProcedure getStoredProcedure(int pid, long txNum) {
		DdStoredProcedure sp;
		switch (TransactionType.values()[pid]) {
		case NEW_ORDER:
			sp = new NewOrderProc(txNum);
			break;
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new DdTestbedLoaderProc(txNum);
			break;
		case FULL_TABLE_SCAN:
			sp = new FullTableScanProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		case PAYMENT:
			sp = new PaymentProc(txNum);
			break;
		case ORDER_STATUS:
			sp = new OrderStatusProc(txNum);
			break;
		case DELIVERY:
			sp = new DeliveryProc(txNum);
			break;
		case STOCK_LEVEL:
			sp = new StockLevelProc(txNum);
			break;
		case MICRO_BENCHMARK:
			sp = new MicroBenchmarkProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException();
		}
		return sp;
	}
}
