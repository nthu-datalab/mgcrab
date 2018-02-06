package netdb.software.benchmark.tpcc.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.remote.storedprocedure.SpStartUp;
import org.vanilladb.core.server.VanillaDb;

public class VanillaDbStartUp implements SutStartUp {
	private static Logger logger = Logger.getLogger(VanillaDbStartUp.class
			.getName());

	public void startup(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");

		VanillaDb.init(args[0], 1);
		if (logger.isLoggable(Level.INFO))
			logger.info("tpcc benchmark vanilladb server ready");
		try {
			SpStartUp.startUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
