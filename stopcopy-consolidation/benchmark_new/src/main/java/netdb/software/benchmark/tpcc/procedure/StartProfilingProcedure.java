package netdb.software.benchmark.tpcc.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;

public class StartProfilingProcedure {

	public SpResultSet execute() {
		boolean isCommitted = true;

		try {
			VanillaDb.initAndStartProfiler();
			System.out.println("start profile");
		} catch (Exception e) {
			e.printStackTrace();
			isCommitted = false;
		}

		// Return the result
		Schema sch = new Schema();
		Type t = Type.VARCHAR(10);
		sch.addField("status", t);
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, t));
		return new SpResultSet(sch, rec);
	}
}