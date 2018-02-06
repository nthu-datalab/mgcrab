package netdb.software.benchmark.tpcc.remote;

import java.sql.SQLException;

public interface SutDriver {

	SutConnection connectToSut(Object... args) throws SQLException;

}
