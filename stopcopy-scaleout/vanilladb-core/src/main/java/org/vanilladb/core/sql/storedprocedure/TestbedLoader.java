package org.vanilladb.core.sql.storedprocedure;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Example stored procedure.
 */
public class TestbedLoader implements StoredProcedure {
	private static Logger logger = Logger.getLogger(TestbedLoader.class
			.getName());

	public TestbedLoader() {

	}

	@Override
	public void prepare(Object... pars) {
		// no pars
	}

	@Override
	public SpResultSet execute() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Execute proc: testbed loader...");

		// Step 0: Parse the input parameter
		/*
		 * TODO: Define your input parameter of this stored procedure. For
		 * example, "[#warehouse] [#customer] [#items]".
		 */

		// Step 1: Create new transaction
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);

		// Step 2: Declare the read/write set

		// Step 3: Conservative ordered locking

		/*
		 * TODO: Modify the concurrency manager and lock table to provide method
		 * to conservatively lock read/write set.
		 */

		// Step 4: Execute stored procedure logic
		try {
			// Create schema

			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		}
		return null;
	}
}
