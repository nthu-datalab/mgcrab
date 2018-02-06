package netdb.software.benchmark.tpce.remote;

import java.sql.SQLException;

public interface SutDriver {

	SutConnection connectToSut(Object... args) throws SQLException;

}
