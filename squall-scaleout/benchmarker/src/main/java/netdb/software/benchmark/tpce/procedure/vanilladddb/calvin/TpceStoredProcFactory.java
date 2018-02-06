package netdb.software.benchmark.tpce.procedure.vanilladddb.calvin;

import netdb.software.benchmark.tpce.TransactionType;

import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedureFactory;

public class TpceStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		TransactionType type = TransactionType.values()[pid];
		switch (type) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc(txNum);
			break;
		case TRADE_ORDER:
			sp = new TradeOrderProc(txNum);
			break;
		case TRADE_RESULT:
			sp = new TradeResultProc(txNum);
			break;
		case START_PROFILING:
			System.out.println("Start profiling");
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			System.out.println("Stop profiling");
			sp = new StopProfilingProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException();
		}
		return sp;
	}
}