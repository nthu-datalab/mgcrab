package netdb.software.benchmark.tpcc.remote.vanilladddb;

import java.sql.SQLException;

import netdb.software.benchmark.tpcc.App;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutDriver;

import org.vanilladb.dd.remote.groupcomm.client.BatchGcConnection;
import org.vanilladb.dd.remote.groupcomm.client.BatchGcDriver;

public class VanillaDdDbDriver implements SutDriver {
	private static final BatchGcConnection conn;

	static {
		BatchGcDriver driver = new BatchGcDriver(App.myNodeId);
		conn = driver.init();
	}

	public SutConnection connectToSut(Object... args) throws SQLException {
		try {
			// all rtes share the same comm. instance
			return new VanillaDdDbConnection(conn, (Integer) args[0]);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
	}
}
