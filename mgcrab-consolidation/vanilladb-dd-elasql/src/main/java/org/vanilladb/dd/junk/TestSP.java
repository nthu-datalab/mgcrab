package org.vanilladb.dd.junk;

import java.sql.Connection;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class TestSP implements DdStoredProcedure {
	private Transaction tx;
	// private Object[] pars;
	private final String TPCC_TABLE_DDL = "CREATE TABLE warehouse ( w_id INT, w_name VARCHAR(10), "
			+ "w_street_1 VARCHAR(20), w_street_2 VARCHAR(20), w_city VARCHAR(20), "
			+ "w_state VARCHAR(2), w_zip VARCHAR(9), w_tax DOUBLE,  w_ytd DOUBLE )";
	private final String CMD = "INSERT INTO warehouse(w_id, w_name, w_street_1, w_street_2,"
			+ " w_city, w_state, w_zip, w_tax, w_ytd) VALUES (1, 'ware1', 'abc', 'abcc', 'cccc', 'AC', 'zzzzz', 98945, 98999)";

	public TestSP(long txNum, Object... pars) {
		tx = VanillaDdDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		// this.pars = pars;
	}

	public SpResultSet execute(Object... pars) {
		// VanillaDb.planner().executeUpdate(TPCC_TABLE_DDL, tx);
		VanillaDb.planner().executeUpdate(CMD, tx);
		tx.commit();
		return createResultSet();
	}

	public Transaction startTransaction() {
		return tx;
	}

	public String[] getReadTables() {
		return null;
	}

	public String[] getWriteTables() {
		String[] wt = { "warehouse" };
		return wt;
	}

	private SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		Type statusMsgType = Type.VARCHAR(30);
		sch.addField("status", statusType);
		sch.addField("status_msg", statusMsgType);

		SpResultRecord rec = new SpResultRecord();
		rec.setVal("status", new VarcharConstant("finn~~~~~", statusType));
		rec.setVal("status_msg", new VarcharConstant("yoyoyo", statusMsgType));

		return new SpResultSet(sch, rec);
	}

	public void prepare(Object... pars) {
		// TODO Auto-generated method stub

	}

	public SpResultSet execute() {
		// TODO Auto-generated method stub
		return null;
	}

	public void requestConservativeLocks() {
		// TODO Auto-generated method stub

	}
}
