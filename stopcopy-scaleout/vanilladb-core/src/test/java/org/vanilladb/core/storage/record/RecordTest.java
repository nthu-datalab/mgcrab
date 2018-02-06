package org.vanilladb.core.storage.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;
import static org.vanilladb.core.storage.record.RecordPage.EMPTY;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgrImpl;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class RecordTest {
	private static Logger logger = Logger.getLogger(RecordTest.class.getName());

	private static String tableName1 = "testcourse1",
			tableName2 = "testcourse2";
	private static Schema schema;
	private static TableInfo ti1, ti2;
	private BufferMgrImpl bufferMgr = VanillaDb.bufferMgr();

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", BIGINT);
		ti1 = new TableInfo(tableName1, schema);
		ti2 = new TableInfo(tableName2, schema);
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN RECORD TEST");
	}

	@Before
	public void setup() {

	}

	@Test
	public void testReadOnly() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		String fileName = ti1.fileName();
		TestFormatter fmtr = new TestFormatter(ti1);
		Buffer buff = bufferMgr.pinNew(fileName, fmtr,
				tx.getTransactionNumber());
		bufferMgr.unpin(tx.getTransactionNumber(), buff);
		RecordPage rp = new RecordPage(buff.block(), ti1, tx, true);
		try {
			rp.insertIntoNextEmptySlot();
			fail("*****RecordTest: bad readOnly");
		} catch (UnsupportedOperationException e) {

		}
		rp.close();
		tx.rollback();
	}

	@Test
	public void testRecordPage() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		String fileName = ti1.fileName();
		TestFormatter fmtr = new TestFormatter(ti1);
		Buffer buff = bufferMgr.pinNew(fileName, fmtr,
				tx.getTransactionNumber());
		bufferMgr.unpin(tx.getTransactionNumber(), buff);
		RecordPage rp = new RecordPage(buff.block(), ti1, tx, true);
		int startid = 0;
		BlockId blk = buff.block();
		RecordId dummyFreeSlot = new RecordId(new BlockId(fileName, -1), -1);
		// Part 0: Delete existing records (if any)
		while (rp.next())
			rp.delete(dummyFreeSlot);
		rp = new RecordPage(blk, ti1, tx, true);

		// Part 1: Fill the page with some records
		int id = startid;
		int numinserted = 0;
		while (rp.insertIntoNextEmptySlot()) {
			rp.setVal("cid", new IntegerConstant(id));
			rp.setVal("deptid", new BigIntConstant((id % 3 + 1) * 10));
			rp.setVal("title", new VarcharConstant("course" + id));
			id++;
			numinserted++;
		}
		rp.close();

		// Part 2: Retrieve the records
		rp = new RecordPage(blk, ti1, tx, true);
		id = startid;
		while (rp.next()) {
			long deptid = (Long) rp.getVal("deptid").asJavaVal();
			int cid = (Integer) rp.getVal("cid").asJavaVal();
			String title = (String) rp.getVal("title").asJavaVal();
			assertTrue("*****RecordTest: bad page read",
					cid == id && title.equals("course" + id)
							&& deptid == (id % 3 + 1) * 10);
			id++;
		}
		rp.close();

		// Part 3: Modify the records
		rp = new RecordPage(blk, ti1, tx, true);
		id = startid;
		int numdeleted = 0;
		while (rp.next()) {
			if (rp.getVal("deptid").equals(new BigIntConstant(30))) {
				rp.delete(dummyFreeSlot);
				numdeleted++;
			}
		}
		rp.close();
		assertEquals("*****RecordTest: deleted wrong records from page",
				numinserted / 3, numdeleted);

		rp = new RecordPage(blk, ti1, tx, true);
		while (rp.next()) {
			assertNotSame("*****RecordTest: bad page delete", (Integer) 30,
					(Long) rp.getVal("deptid").asJavaVal());
		}
		rp.close();
		tx.rollback();
	}

	@Test
	public void testRecordFile() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);

		// initial header page

		FileHeaderFormatter fhf = new FileHeaderFormatter();
		Buffer buff = VanillaDb.bufferMgr().pinNew(ti2.fileName(), fhf,
				tx.getTransactionNumber());
		VanillaDb.bufferMgr().unpin(tx.getTransactionNumber(), buff);

		RecordFile rf = ti2.open(tx, true);
		rf.beforeFirst();
		int max = 300;

		// Part 0: Delete existing records (if any)
		while (rf.next())
			rf.delete();
		rf.close();

		// Part 1: Fill the file with lots of records
		rf = ti2.open(tx, true);
		for (int id = 0; id < max; id++) {
			rf.insert();
			rf.setVal("cid", new IntegerConstant(id));
			rf.setVal("title", new VarcharConstant("course" + id));
			rf.setVal("deptid", new BigIntConstant((id % 3 + 1) * 10));
		}
		rf.close();

		// Part 2: Retrieve the records
		int id = 0;
		rf = ti2.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			int cid = (Integer) rf.getVal("cid").asJavaVal();
			String title = (String) rf.getVal("title").asJavaVal();
			long deptid = (Long) rf.getVal("deptid").asJavaVal();
			assertTrue("*****RecordTest: bad file read",
					cid == id && title.equals("course" + id)
							&& deptid == (id % 3 + 1) * 10);
			id++;
		}
		rf.close();
		assertEquals("*****RecordTest: wrong number of records", max, id);

		// Part 3: Delete some of the records
		rf = ti2.open(tx, true);
		rf.beforeFirst();
		int numdeleted = 0;
		while (rf.next()) {
			if (rf.getVal("deptid").equals(new BigIntConstant(30))) {
				rf.delete();
				numdeleted++;
			}
		}
		assertEquals("*****RecordTest: wrong number of deletions", max / 3,
				numdeleted);

		// test that the deletions occurred
		rf.beforeFirst();
		while (rf.next()) {
			assertNotSame("*****RecordTest: not enough deletions", (Long) 30L,
					(Long) rf.getVal("deptid").asJavaVal());
		}
		rf.close();

		rf = ti2.open(tx, true);
		for (int i = 301; i < 405; i++) {
			rf.insert();
			rf.setVal("cid", new IntegerConstant(i));
			rf.setVal("title", new VarcharConstant("course" + id));
			rf.setVal("deptid", new BigIntConstant((id % 3 + 1) * 10));
		}
		rf.close();

		tx.rollback();
	}

}

class TestFormatter implements PageFormatter {
	private static int intTypeCapacity = Page.maxSize(INTEGER);
	private TableInfo ti;

	public TestFormatter(TableInfo ti) {
		this.ti = ti;
	}

	@Override
	public void format(Page page) {
		int recsize = RecordPage.recordSize(ti.schema()) + intTypeCapacity;
		for (int pos = 0; pos + recsize <= BLOCK_SIZE; pos += recsize) {
			page.setVal(pos, new IntegerConstant(EMPTY));
			makeDefaultRecord(page, pos);
		}
	}

	private void makeDefaultRecord(Page page, int pos) {
		Map<String, Integer> myOffsetMap = RecordPage.offsetMap(ti.schema());
		for (String fldname : ti.schema().fields()) {
			int offset = myOffsetMap.get(fldname);
			if (ti.schema().type(fldname).equals(INTEGER))
				page.setVal(pos + intTypeCapacity + offset,
						new IntegerConstant(0));
			else if (ti.schema().type(fldname).equals(BIGINT))
				page.setVal(pos + intTypeCapacity + offset, new BigIntConstant(
						0));
			else
				page.setVal(pos + intTypeCapacity + offset,
						new VarcharConstant(""));
		}
	}
}
