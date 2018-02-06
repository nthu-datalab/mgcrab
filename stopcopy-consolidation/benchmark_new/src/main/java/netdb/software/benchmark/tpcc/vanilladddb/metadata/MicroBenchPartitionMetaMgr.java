package netdb.software.benchmark.tpcc.vanilladddb.metadata;

import netdb.software.benchmark.tpcc.TpccConstants;

import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class MicroBenchPartitionMetaMgr extends PartitionMetaMgr {
	
	// XXX: For YCSB and migration
//	public static final int ITEM_PER_PARTITION = TpccConstants.NUM_ITEMS / PartitionMetaMgr.NUM_PARTITIONS;
//	public static final int RECORD_PER_PARTITION = TpccConstants.YCSB_NUM_RECORDS / (PartitionMetaMgr.NUM_PARTITIONS - 1);
	
	public static int getRangeIndex(RecordKey key) {
		return getRangeIndex(Integer.parseInt(key.getKeyVal("ycsb_id").toString()));
	}
	
	public static int getRangeIndex(int id) {
		return (id - 1) / TpccConstants.YCSB_MAX_RECORD_PER_PART;
	}
	
	public MicroBenchPartitionMetaMgr() {
	}

	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {

		// For a special type of record
		if (key.getTableName().equals("notification"))
			return -1;
		
		if (VanillaDdDb.migrationMgr().keyIsInMigrationRange(key)) {
			if (VanillaDdDb.migrationMgr().isMigrated())
				return VanillaDdDb.migrationMgr().getDestPartition();
			else
				return VanillaDdDb.migrationMgr().getSourcePartition();
		}

		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */
//		int iid = (int) key.getKeyVal("i_id").asJavaVal();
//		return (iid - 1) / ITEM_PER_PARTITION;
		return getRangeIndex(key);
	}
}
