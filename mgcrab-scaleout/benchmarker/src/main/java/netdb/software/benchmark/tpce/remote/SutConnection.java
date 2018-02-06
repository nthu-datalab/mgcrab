package netdb.software.benchmark.tpce.remote;

import java.sql.SQLException;

public interface SutConnection {

	SutResultSet callStoredProc(int pid, Object... pars) throws SQLException;

}
