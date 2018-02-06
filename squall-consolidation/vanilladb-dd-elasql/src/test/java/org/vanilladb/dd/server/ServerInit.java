package org.vanilladb.dd.server;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class ServerInit {
	private static Logger logger = Logger.getLogger(ServerInit.class.getName());

	public static int courseMax = 300, studentMax = 900, deptMax = 40,
			sectMax = 1200, enrollMax = 2000;
	public static String dbName = "testvanilladddb";

	/**
	 * Initiates {@link VanillaDb} and sets up a database for testing.
	 * 
	 * <p>
	 * Note that for each test class, members (e.g., static fields,
	 * constructors, etc) of all VanillaDb classes should be accessed after
	 * calling this method to ensure the proper class loading.
	 * </p>
	 */
	public static void initData() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN INITIALIZATION");

		VanillaDdDb.init(dbName, 0);

		if (VanillaDdDb.fileMgr().isNew()) {
			if (logger.isLoggable(Level.INFO))
				logger.info("loading data");
			CatalogMgr md = VanillaDdDb.catalogMgr();
			Transaction tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false);

			// create and populate the course table
			Schema sch = new Schema();
			sch.addField("cid", INTEGER);
			sch.addField("title", VARCHAR(20));
			sch.addField("deptid", INTEGER);
			md.createTable("course", sch, tx);
			TableInfo ti = md.getTableInfo("course", tx);
			md.createIndex("course-id-index", "course", "cid", Index.IDX_BTREE,
					tx);
			Index idx = md.getIndexInfo("course", tx).get("cid").open(tx);

			RecordFile rf = ti.open(tx, true);
			for (int id = 0; id < courseMax; id++) {
				rf.insert();
				RecordId rid = rf.currentRecordId();
				IntegerConstant cid = new IntegerConstant(id);
				rf.setVal("cid", cid);
				rf.setVal("title", new VarcharConstant("course" + id));
				rf.setVal("deptid", new IntegerConstant(id % deptMax));
				idx.insert(cid, rid);
			}
			rf.close();
			idx.close();

			// refresh the statistical information after populating this table
			VanillaDdDb.statMgr().getTableStatInfo(ti, tx);

			// create and populate the student table
			sch = new Schema();
			sch.addField("sid", INTEGER);
			sch.addField("sname", VARCHAR(10));
			sch.addField("majorid", INTEGER);
			sch.addField("gradyear", INTEGER);
			md.createTable("student", sch, tx);
			ti = md.getTableInfo("student", tx);
			md.createIndex("student-id-index", "student", "sid",
					Index.IDX_BTREE, tx);
			idx = md.getIndexInfo("student", tx).get("sid").open(tx);

			rf = ti.open(tx, true);
			for (int id = 0; id < studentMax; id++) {
				rf.insert();
				RecordId rid = rf.currentRecordId();
				IntegerConstant sid = new IntegerConstant(id);
				rf.setVal("sid", sid);
				rf.setVal("sname", new VarcharConstant("student" + id));
				rf.setVal("majorid", new IntegerConstant(id % deptMax));
				rf.setVal("gradyear", new IntegerConstant((id % 50) + 1960));
				idx.insert(sid, rid);
			}
			rf.close();
			idx.close();
			// refresh the statistical information after populating this table
			VanillaDdDb.statMgr().getTableStatInfo(ti, tx);

			// create and populate the dept table
			sch = new Schema();
			sch.addField("did", INTEGER);
			sch.addField("dname", VARCHAR(8));
			md.createTable("dept", sch, tx);
			ti = md.getTableInfo("dept", tx);

			rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			for (int id = 0; id < deptMax; id++) {
				rf.insert();
				IntegerConstant did = new IntegerConstant(id);
				rf.setVal("did", did);
				rf.setVal("dname", new VarcharConstant("dept" + id));
			}
			rf.close();
			// refresh the statistical information after populating this table
			VanillaDdDb.statMgr().getTableStatInfo(ti, tx);

			// create and populate the section table
			sch = new Schema();
			sch.addField("sectid", INTEGER);
			sch.addField("prof", VARCHAR(8));
			sch.addField("courseid", INTEGER);
			sch.addField("yearoffered", INTEGER);
			md.createTable("section", sch, tx);
			ti = md.getTableInfo("section", tx);

			rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			for (int id = 0; id < sectMax; id++) {
				rf.insert();
				IntegerConstant sectid = new IntegerConstant(id);
				rf.setVal("sectid", sectid);
				int profnum = id % 20;
				rf.setVal("prof", new VarcharConstant("prof" + profnum));
				rf.setVal("courseid", new IntegerConstant(id % courseMax));
				rf.setVal("yearoffered", new IntegerConstant((id % 50) + 1960));
			}
			rf.close();
			// refresh the statistical information after populating this table
			VanillaDdDb.statMgr().getTableStatInfo(ti, tx);

			// create and populate the enroll table
			sch = new Schema();
			sch.addField("eid", INTEGER);
			sch.addField("grade", VARCHAR(2));
			sch.addField("studentid", INTEGER);
			sch.addField("sectionid", INTEGER);
			md.createTable("enroll", sch, tx);
			ti = md.getTableInfo("enroll", tx);

			rf = ti.open(tx, true);
			rf.beforeFirst();
			while (rf.next())
				rf.delete();
			rf.close();

			rf = ti.open(tx, true);
			String[] grades = new String[] { "A", "B", "C", "D", "F" };
			for (int id = 0; id < enrollMax; id++) {
				rf.insert();
				IntegerConstant eid = new IntegerConstant(id);
				rf.setVal("eid", eid);
				rf.setVal("grade", new VarcharConstant(grades[id % 5]));
				rf.setVal("studentid", new IntegerConstant(id % studentMax));
				rf.setVal("sectionid", new IntegerConstant(id % sectMax));
			}
			rf.close();
			// refresh the statistical information after populating this table
			VanillaDdDb.statMgr().getTableStatInfo(ti, tx);

			tx.commit();

			// add a checkpoint record to limit rollback
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			RecoveryMgr.recover(tx);
			tx.commit();
		}
	}
}