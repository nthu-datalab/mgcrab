package org.vanilladb.core.storage.index.btree;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_BTREE;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class BTreeIndexTest {
	private static Logger logger = Logger.getLogger(BTreeIndexTest.class
			.getName());
	private static CatalogMgr md;
	private static String dataTableName = "_tempBtreeData";

	@BeforeClass
	public static void init() {
		ServerInit.initData();
		md = VanillaDb.catalogMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BTREEINDEX TEST");

		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", INTEGER);
		schema.addField("majorid", BIGINT);
		md.createTable(dataTableName, schema, tx);
		md.createIndex("_tempI1", dataTableName, "cid", IDX_BTREE, tx);
		md.createIndex("_tempI2", dataTableName, "title", IDX_BTREE, tx);
		md.createIndex("_tempI3", dataTableName, "deptid", IDX_BTREE, tx);
		md.createIndex("_tempI4", dataTableName, "majorid", IDX_BTREE, tx);
		tx.commit();
	}

	@Before
	public void setup() {
		// turn off logging to speed up testing process
		RecoveryMgr.logSetVal(false);
	}

	@AfterClass
	public static void clear() {
		RecoveryMgr.logSetVal(true);
	}

	@Test
	public void testBasicOperation() {
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

		RecordId rid2 = new RecordId(blk, 9);
		Constant int7 = new IntegerConstant(7);
		cidIndex.insert(int7, rid2);

		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		int k = 0;
		while (cidIndex.next())
			k++;
		assertTrue("*****BTreeIndexTest: bad insert", k == 10);

		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		cidIndex.next();
		assertTrue("*****BTreeIndexTest: bad read index", cidIndex
				.getDataRecordId().equals(rid2));

		for (int i = 0; i < 10; i++)
			cidIndex.delete(int5, records[i]);
		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		cidIndex.delete(int7, rid2);
		cidIndex.close();
		tx.commit();
	}

	@Test
	public void testVarcharKey() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index cidIndex = idxmap.get("title").open(tx);

		BlockId blk = new BlockId(dataTableName + ".tbl", 0);

		int repeat = 1000;
		String str1 = "BAEBAEBAEBASAEBASE";
		String str3 = "AAEBAEBAEBASAEBASZ";
		String str4 = "BAEBAEBAEBASAEBASZ1";
		String str2 = "KARBAEBAEBASAEBASE";
		Constant key1 = new VarcharConstant(str1, VARCHAR(20));
		Constant key2 = new VarcharConstant(str2, VARCHAR(20));
		Constant key3 = new VarcharConstant(str3, VARCHAR(20));
		Constant key4 = new VarcharConstant(str4, VARCHAR(20));

		for (int i = 0; i < repeat; i++) {
			cidIndex.insert(key1, new RecordId(blk, i));
			cidIndex.insert(key2, new RecordId(blk, repeat + i));
			cidIndex.insert(key3, new RecordId(blk, repeat * 2 + i));
			cidIndex.insert(key4, new RecordId(blk, repeat * 3 + i));
		}

		cidIndex.beforeFirst(ConstantRange.newInstance(key1));
		int j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: varchar selection", j == repeat);

		for (int i = 0; i < repeat; i++) {
			cidIndex.delete(key1, new RecordId(blk, i));
			cidIndex.delete(key2, new RecordId(blk, repeat + i));
			cidIndex.delete(key3, new RecordId(blk, repeat * 2 + i));
			cidIndex.delete(key4, new RecordId(blk, repeat * 3 + i));
		}

		cidIndex.close();
		tx.commit();
	}

	@Test
	public void testDir() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index cidIndex = idxmap.get("majorid").open(tx);
		BlockId blk1 = new BlockId(dataTableName + ".tbl", 0);
		int maxValue = 250; // 40000000
		/*
		 * for 4K block, int value: repeat same val 250 times may create
		 * overflow blk if repeat =200, the btree will create new node at k< 250
		 * and k<250*250. bigint: 200 will overflow. make new node at <40000
		 */
		int repeat = 170;
		for (int k = 0; k < maxValue; k++) {
			Constant con = new BigIntConstant(k);
			for (int i = 0; i < repeat; i++) {
				cidIndex.insert(con, new RecordId(blk1, k * repeat + i));
			}
		}

		Constant int100 = new IntegerConstant(100);
		cidIndex.beforeFirst(ConstantRange.newInstance(int100));
		int j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad equal with", j == repeat);

		for (int i = 0; i < repeat; i++) {
			cidIndex.delete(int100, new RecordId(blk1, 100 * repeat + i));
		}
		cidIndex.beforeFirst(ConstantRange.newInstance(int100));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);
	}

	@Test
	public void testBTreeIndex() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index cidIndex = idxmap.get("deptid").open(tx);
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);
		BlockId blk1 = new BlockId(dataTableName + ".tbl", 23);
		int maxValue = 300;
		int repeat = 200;
		for (int k = 0; k < maxValue; k++) {
			for (int i = 0; i < repeat; i++) {
				cidIndex.insert(new IntegerConstant(k), new RecordId(blk, k
						* repeat + i));
			}
		}

		int count = 0;
		Constant int7 = new IntegerConstant(7);
		while (count < 500) {
			cidIndex.insert(int7, new RecordId(blk1, 2500 + count));
			count++;
		}

		// test larger than 50
		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(50),
				false, null, false));
		int j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad > selection", j == (maxValue - 51)
				* repeat);

		Constant int5 = new IntegerConstant(5);
		// test less than
		cidIndex.beforeFirst(ConstantRange
				.newInstance(null, false, int5, false));
		j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad < selection", j == (5 * repeat));

		// test equality
		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad equal with", j == repeat);

		// test delete
		for (int k = 0; k < maxValue; k++) {
			for (int i = 0; i < repeat; i++) {
				cidIndex.delete(new IntegerConstant(k), new RecordId(blk, k
						* repeat + i));
			}
		}
		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(5)));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		count = 0;
		while (count < 500) {
			cidIndex.delete(int7, new RecordId(blk1, 2500 + count));
			count++;
		}
		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		cidIndex.close();
		tx.commit();
	}
}
