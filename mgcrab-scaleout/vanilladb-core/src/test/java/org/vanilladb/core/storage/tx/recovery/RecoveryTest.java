package org.vanilladb.core.storage.tx.recovery;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_BTREE;
import static org.vanilladb.core.storage.index.Index.IDX_HASH;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgrImpl;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class RecoveryTest {
	private static Logger logger = Logger.getLogger(RecoveryTest.class
			.getName());
	/**
	 * Filename cannot start with "_temp" otherwise the operations over the file
	 * will be ignored by {@link RecoveryMgr}
	 */
	private static String fileName = "recoverytest.0";
	private static String dataTableName = "recoverytest";
	private static CatalogMgr md;

	private static BlockId blk;
	private static BufferMgrImpl bm;

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		blk = new BlockId(fileName, 12);
		bm = VanillaDb.bufferMgr();
		md = VanillaDb.catalogMgr();
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);

		Schema schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", INTEGER);
		schema.addField("majorid", BIGINT);
		md.createTable(dataTableName, schema, tx);
		md.createIndex("index_cid", dataTableName, "cid", IDX_BTREE, tx);
		md.createIndex("index_deptid", dataTableName, "deptid", IDX_HASH, tx);
		tx.commit();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN RECOVERY TEST");
	}

	@Before
	public void setup() {
		// reset initial values in the block
		long txNum = 250;
		Buffer buff = bm.pin(blk, txNum);
		buff.setVal(4, new IntegerConstant(9876), txNum, -1);
		buff.setVal(20, new VarcharConstant("abcdefg"), txNum, -1);
		buff.setVal(40, new VarcharConstant("hijk"), txNum, -1);
		buff.setVal(104, new IntegerConstant(9999), txNum, -1);
		buff.setVal(120, new VarcharConstant("gfedcba"), txNum, -1);
		buff.setVal(140, new VarcharConstant("kjih"), txNum, -1);
		buff.setVal(204, new IntegerConstant(1415), txNum, -1);
		buff.setVal(220, new VarcharConstant("pifo"), txNum, -1);
		buff.setVal(240, new VarcharConstant("urth"), txNum, -1);
		buff.setVal(304, new IntegerConstant(9265), txNum, -1);
		buff.setVal(320, new VarcharConstant("piei"), txNum, -1);
		buff.setVal(340, new VarcharConstant("ghth"), txNum, -1);
		bm.flushAll(txNum);
		bm.unpin(txNum, buff);
	}

	@Test
	public void testRollback() {
		// log and make changes to the block's values

		LinkedList<BlockId> blklist = new LinkedList<BlockId>();
		blklist.add(blk);
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm = tx.recoveryMgr();
		long txNum = tx.getTransactionNumber();
		Buffer buff = bm.pin(blk, txNum);
		long lsn = rm.logSetVal(buff, 4, new IntegerConstant(1234));
		buff.setVal(4, new IntegerConstant(1234), txNum, lsn);
		lsn = rm.logSetVal(buff, 20, new VarcharConstant("xyz"));
		buff.setVal(20, new VarcharConstant("xyz"), txNum, lsn);

		bm.unpin(txNum, buff);
		bm.flushAll(txNum);

		// verify that the changes got made
		buff = bm.pin(blk, txNum);
		assertTrue(
				"*****RecoveryTest: rollback changes not made",
				buff.getVal(4, INTEGER).equals(new IntegerConstant(1234))
						&& ((String) buff.getVal(20, VARCHAR).asJavaVal())
								.equals("xyz"));
		bm.unpin(txNum, buff);

		rm.onTxRollback(tx);

		// verify that they got rolled back
		buff = bm.pin(blk, txNum);
		int ti = (Integer) buff.getVal(4, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(20, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad rollback",
				ti == 9876 && ts.equals("abcdefg"));
		bm.unpin(txNum, buff);
	}

	@Test
	public void testRecover() {
		// use different txs to log and make changes to those values
		Transaction tx1 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm1 = tx1.recoveryMgr();
		Transaction tx2 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm2 = tx2.recoveryMgr();
		Transaction tx3 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm3 = tx3.recoveryMgr();
		long txNum1 = tx1.getTransactionNumber();
		long txNum2 = tx2.getTransactionNumber();
		long txNum3 = tx3.getTransactionNumber();

		Buffer buff = bm.pin(blk, txNum1);
		bm.pin(blk, txNum2);
		bm.pin(blk, txNum3);
		long lsn = rm1.logSetVal(buff, 104, new IntegerConstant(1234));
		buff.setVal(104, new IntegerConstant(1234), txNum1, lsn);
		lsn = rm2.logSetVal(buff, 120, new VarcharConstant("xyz"));
		buff.setVal(120, new VarcharConstant("xyz"), txNum2, lsn);
		lsn = rm3.logSetVal(buff, 140, new VarcharConstant("rst"));
		buff.setVal(140, new VarcharConstant("rst"), txNum3, lsn);
		bm.unpin(txNum1, buff);
		bm.unpin(txNum2, buff);
		bm.unpin(txNum3, buff);

		// verify that the changes got made
		buff = bm.pin(blk, txNum1);
		assertTrue(
				"*****RecoveryTest: recovery changes not made",
				buff.getVal(104, INTEGER).equals(new IntegerConstant(1234))
						&& ((String) buff.getVal(120, VARCHAR).asJavaVal())
								.equals("xyz")
						&& ((String) buff.getVal(140, VARCHAR).asJavaVal())
								.equals("rst"));
		bm.unpin(txNum1, buff);

		rm2.onTxCommit(tx2);

		Transaction recoveryTx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.recover(recoveryTx);
		// verify that tx1 and tx3 got rolled back
		buff = bm.pin(blk, txNum1);
		int ti = (Integer) buff.getVal(104, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(120, VARCHAR).asJavaVal();
		String ts2 = (String) buff.getVal(140, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad recovery",
				ti == 9999 && ts.equals("xyz") && ts2.equals("kjih"));
		bm.unpin(txNum1, buff);
	}

	@Test
	public void testCheckpoint() {
		// use different txs to log and make changes to those values
		Transaction tx1 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm1 = tx1.recoveryMgr();
		Transaction tx2 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm2 = tx2.recoveryMgr();
		Transaction tx3 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm3 = tx3.recoveryMgr();
		Transaction tx4 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm4 = tx4.recoveryMgr();

		long txNum1 = tx1.getTransactionNumber();
		long txNum2 = tx2.getTransactionNumber();
		long txNum3 = tx3.getTransactionNumber();
		long txNum4 = tx4.getTransactionNumber();

		Buffer buff = bm.pin(blk, txNum1);
		bm.pin(blk, txNum2);
		bm.pin(blk, txNum3);
		bm.pin(blk, txNum4);

		long lsn = rm1.logSetVal(buff, 204, new IntegerConstant(3538));
		buff.setVal(204, new IntegerConstant(3538), txNum1, lsn);
		lsn = rm2.logSetVal(buff, 220, new VarcharConstant("twel"));
		buff.setVal(220, new VarcharConstant("twel"), txNum2, lsn);
		lsn = rm3.logSetVal(buff, 240, new VarcharConstant("tfth"));
		buff.setVal(240, new VarcharConstant("tfth"), txNum3, lsn);
		lsn = rm4.logSetVal(buff, 304, new IntegerConstant(9323));
		buff.setVal(304, new IntegerConstant(9323), txNum4, lsn);

		bm.unpin(txNum2, buff);
		bm.unpin(txNum3, buff);

		rm2.onTxCommit(tx2);
		rm3.onTxRollback(tx3);

		// make checkpoint
		Transaction chkpnt = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		VanillaDb.txMgr().createCheckpoint(chkpnt);
		chkpnt.concurrencyMgr().onTxCommit(chkpnt);

		bm.unpin(txNum1, buff);
		bm.unpin(txNum4, buff);
		rm1.onTxCommit(tx1);

		Transaction tx5 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm5 = tx5.recoveryMgr();
		Transaction tx6 = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm6 = tx6.recoveryMgr();

		long txNum5 = tx5.getTransactionNumber();
		long txNum6 = tx6.getTransactionNumber();

		bm.pin(blk, txNum5);
		bm.pin(blk, txNum6);

		lsn = rm5.logSetVal(buff, 320, new VarcharConstant("sixt"));
		buff.setVal(320, new VarcharConstant("sixt"), txNum5, lsn);
		lsn = rm6.logSetVal(buff, 340, new VarcharConstant("eenth"));
		buff.setVal(340, new VarcharConstant("eenth"), txNum6, lsn);

		bm.unpin(txNum5, buff);
		bm.unpin(txNum6, buff);

		rm5.onTxCommit(tx5);
		// rm6.onTxRollback(tx6); make the tx6 as an uncompleted tx

		Transaction recoveryTx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr.recover(recoveryTx);

		// verify that tx3, tx4 and tx6 got rolled back
		buff = bm.pin(blk, txNum1);
		int ti1 = (Integer) buff.getVal(204, INTEGER).asJavaVal();
		String ts2 = (String) buff.getVal(220, VARCHAR).asJavaVal();
		String ts3 = (String) buff.getVal(240, VARCHAR).asJavaVal();
		int ti4 = (Integer) buff.getVal(304, INTEGER).asJavaVal();
		String ts5 = (String) buff.getVal(320, VARCHAR).asJavaVal();
		String ts6 = (String) buff.getVal(340, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad checkpoint recovery", ti1 == 3538
				&& ts2.equals("twel") && ts3.equals("urth") && ti4 == 9265
				&& ts5.equals("sixt") && ts6.equals("ghth"));
		bm.unpin(txNum1, buff);
	}

	@Test
	public void testBTreeIndexRecovery() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index cidIndex = idxmap.get("cid").open(tx);
		RecordId[] records = new RecordId[10];
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);
		Constant int5 = new IntegerConstant(5);
		for (int i = 0; i < 10; i++) {
			records[i] = new RecordId(blk, i);
			cidIndex.insert(int5, records[i]);
		}

		RecordId rid2 = new RecordId(blk, 19);
		Constant int7 = new IntegerConstant(7);
		cidIndex.insert(int7, rid2);

		cidIndex.close();
		tx.commit();

		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				false);
		RecoveryMgr.recover(tx);
		tx.commit();

		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				true);
		idxmap = md.getIndexInfo(dataTableName, tx);
		cidIndex = idxmap.get("cid").open(tx);
		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		int k = 0;
		while (cidIndex.next())
			k++;

		assertTrue("*****RecoveryTest: bad index insertion recovery", k == 10);

		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		cidIndex.next();
		assertTrue("*****RecoveryTest: bad index insertion recovery", cidIndex
				.getDataRecordId().equals(rid2));

		cidIndex.close();
		tx.commit();

		// test roll back deletion on index
		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				false);
		idxmap = md.getIndexInfo(dataTableName, tx);
		cidIndex = idxmap.get("cid").open(tx);
		cidIndex.delete(int7, rid2);

		RecordId rid3 = new RecordId(blk, 999);
		Constant int777 = new IntegerConstant(777);
		cidIndex.insert(int777, rid3);
		cidIndex.close();
		tx.rollback();

		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				true);
		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		cidIndex.next();
		assertTrue("*****RecoveryTest: bad index deletion rollback", cidIndex
				.getDataRecordId().equals(rid2));

		cidIndex.beforeFirst(ConstantRange.newInstance(int777));
		cidIndex.next();
		assertTrue("*****RecoveryTest: bad index insertion rollback",
				!cidIndex.next());
		cidIndex.close();
		tx.commit();
	}

	@Test
	public void testHashIndexRecovery() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index deptidIndex = idxmap.get("deptid").open(tx);
		RecordId[] records = new RecordId[10];
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);
		Constant int5 = new IntegerConstant(5);
		for (int i = 0; i < 10; i++) {
			records[i] = new RecordId(blk, i);
			deptidIndex.insert(int5, records[i]);
		}

		RecordId rid2 = new RecordId(blk, 19);
		Constant int7 = new IntegerConstant(7);
		deptidIndex.insert(int7, rid2);

		deptidIndex.close();
		tx.commit();

		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				false);
		RecoveryMgr.recover(tx);
		tx.commit();

		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				true);
		idxmap = md.getIndexInfo(dataTableName, tx);
		deptidIndex = idxmap.get("deptid").open(tx);
		deptidIndex.beforeFirst(ConstantRange.newInstance(int5));
		int k = 0;
		while (deptidIndex.next())
			k++;
		assertTrue("*****RecoveryTest: bad index insertion recovery", k == 10);

		deptidIndex.beforeFirst(ConstantRange.newInstance(int7));
		deptidIndex.next();
		assertTrue("*****RecoveryTest: bad index insertion recovery",
				deptidIndex.getDataRecordId().equals(rid2));

		deptidIndex.close();
		tx.commit();

		// test roll back deletion on index
		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				false);
		idxmap = md.getIndexInfo(dataTableName, tx);
		deptidIndex = idxmap.get("deptid").open(tx);
		deptidIndex.delete(int7, rid2);

		RecordId rid3 = new RecordId(blk, 999);
		Constant int777 = new IntegerConstant(777);
		deptidIndex.insert(int777, rid3);
		deptidIndex.close();
		tx.rollback();

		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				true);
		deptidIndex.beforeFirst(ConstantRange.newInstance(int7));
		deptidIndex.next();
		assertTrue("*****RecoveryTest: bad index deletion rollback",
				deptidIndex.getDataRecordId().equals(rid2));

		deptidIndex.beforeFirst(ConstantRange.newInstance(int777));
		deptidIndex.next();
		assertTrue("*****RecoveryTest: bad index insertion rollback",
				!deptidIndex.next());

		deptidIndex.close();
		tx.commit();
	}
}
