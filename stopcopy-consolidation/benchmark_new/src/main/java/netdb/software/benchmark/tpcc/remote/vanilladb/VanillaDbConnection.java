package netdb.software.benchmark.tpcc.remote.vanilladb;

import java.sql.SQLException;

import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;

import org.vanilladb.core.remote.storedprocedure.SpConnection;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class VanillaDbConnection implements SutConnection {
	private SpConnection conn;

	public VanillaDbConnection(SpConnection conn) {
		this.conn = conn;
	}

	@Override
	public SutResultSet callStoredProc(int pid, Object... pars)
			throws SQLException {
		try {
			SpResultSet r = conn.callStoredProc(pid, pars);
			return new VanillaDbResultSet(r);
		} catch (SQLException e) {
			throw new SQLException(e);
		}
	}
}
