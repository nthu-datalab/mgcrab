package netdb.software.benchmark.tpcc.vanilladddb.metadata;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class TpccPartitionMetaMgr extends PartitionMetaMgr {
	
	public static final int WAREHOUSE_PER_PART = 10;

	public TpccPartitionMetaMgr() {
	}

	public boolean isFullyReplicated(RecordKey key) {
		return key.getTableName().equals("item");
	}
	
	public static int getWarehouseId(RecordKey key) {
		// For other tables, partitioned by wid
		Constant widCon;
		switch (key.getTableName()) {
		case "warehouse":
			widCon = key.getKeyVal("w_id");
			break;
		case "district":
			widCon = key.getKeyVal("d_w_id");
			break;
		case "stock":
			widCon = key.getKeyVal("s_w_id");
			break;
		case "customer":
			widCon = key.getKeyVal("c_w_id");
			break;
		case "history":
			widCon = key.getKeyVal("h_c_w_id");
			break;
		case "orders":
			widCon = key.getKeyVal("o_w_id");
			break;
		case "new_order":
			widCon = key.getKeyVal("no_w_id");
			break;
		case "order_line":
			widCon = key.getKeyVal("ol_w_id");
			break;
		default:
			throw new IllegalArgumentException("cannot find proper partition rule for key:" + key);
		}
		
		return (Integer) widCon.asJavaVal();
	}

	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for TPC-C testbed. Partitions each
		 * table on warehouse id.
		 */
		
//		System.out.println("Count: " + (accessCount.getAndIncrement()));
//		
		// Return cached partition number
//		if(key.getPartition() != -1)
//			return key.getPartition();
		
		// If is item table, return self node id
		// (items are fully replicated over all partitions)
		if (key.getTableName().equals("item"))
			return VanillaDdDb.serverId();
		
		// For a special type of record
		if (key.getTableName().equals("notification"))
			return -1;
		
		// For other tables, partitioned by wid
		int wid = getWarehouseId(key);
		if (wid > 0) {
			// Return the warehouse id according to the ratio of
			// # of warehouses and # of partition
			//int wPerPart = TpccConstants.NUM_WAREHOUSES / PartitionMetaMgr.NUM_PARTITIONS;
			int wPerPart = WAREHOUSE_PER_PART;

			int targetPartId = (wid - 1) / wPerPart;
			
			if (wid <= WAREHOUSE_PER_PART)
				return 0;
			else if (wid <= WAREHOUSE_PER_PART * 2)
				return VanillaDdDb.migrationMgr().getDestPartition();
			else if (VanillaDdDb.migrationMgr().isMigrated())
				return VanillaDdDb.migrationMgr().getDestPartition();
			else
				return VanillaDdDb.migrationMgr().getSourcePartition();
			
			// If it is in the migration range and migration has been finished
//			if (VanillaDdDb.migrationMgr().keyIsInMigrationRange(key)) {
//				if (VanillaDdDb.migrationMgr().isMigrated())
//					targetPartId = VanillaDdDb.migrationMgr().getDestPartition();
//				else
//					targetPartId = VanillaDdDb.migrationMgr().getSourcePartition();
//			}
//			
//			return targetPartId;
		}
		
		throw new IllegalArgumentException(
				"cannot find proper partition rule for key:" + key);
	}
}
