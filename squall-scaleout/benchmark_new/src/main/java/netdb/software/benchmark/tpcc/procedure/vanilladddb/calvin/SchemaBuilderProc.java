package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;

import netdb.software.benchmark.tpcc.procedure.SchemaBuilderProcParamHelper;

public class SchemaBuilderProc extends AllExecuteProcedure<SchemaBuilderProcParamHelper> {
	private static Logger logger = Logger.getLogger(SchemaBuilderProc.class
			.getName());

	public SchemaBuilderProc(long txNum) {
		super(txNum, new SchemaBuilderProcParamHelper());
		forceReadWriteTx = true;
	}
	
	// XXX: We should lock those tables
	// @Override
	// public void prepareKeys() {
	// localWriteTables.addAll(Arrays.asList(paramHelper.getTableNames()));
	// }

	@Override
	protected void executeSql() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start building schema...");
		
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.planner().executeUpdate(cmd, tx);
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.planner().executeUpdate(cmd, tx);

		if (logger.isLoggable(Level.INFO))
			logger.info("Finish building schema.");
	}
}