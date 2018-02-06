package netdb.software.benchmark.tpce.procedure.vanilladddb.calvin;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpce.procedure.SchemaBuilderProcParamHelper;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;

public class SchemaBuilderProc extends
		CalvinStoredProcedure<SchemaBuilderProcParamHelper> {
	private static Logger logger = Logger.getLogger(SchemaBuilderProc.class
			.getName());

	public SchemaBuilderProc(long txNum) {
		super(txNum, new SchemaBuilderProcParamHelper());
		isMaster = true;
	}

	@Override
	public void prepareKeys() {
		localWriteTables.addAll(Arrays.asList(paramHelper.getTableNames()));
	}

	@Override
	public boolean executeSql() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create schema for tpce testbed...");
		
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.planner().executeUpdate(cmd, tx);
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.planner().executeUpdate(cmd, tx);

		return true;
	}
}
