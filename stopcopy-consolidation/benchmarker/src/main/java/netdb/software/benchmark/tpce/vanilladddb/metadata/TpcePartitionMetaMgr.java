package netdb.software.benchmark.tpce.vanilladddb.metadata;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class TpcePartitionMetaMgr extends PartitionMetaMgr {

	public TpcePartitionMetaMgr() {
	}

	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
//		long cid = -1;
//		if(key.getTableName() == "customer")
//			cid = (long) key.getKeyVal("c_id").asJavaVal();
//		
//		if(key.getTableName() == "customer_account")
//			cid = ((long) key.getKeyVal("ca_id").asJavaVal())/10 + 1;
//		
//		if (cid != -1)
//			return (int)(cid % PartitionMetaMgr.NUM_PARTITIONS);
		
//		if (key.getTableName() == "trade_type")
//			return VanillaDdDb.serverId();
		
		String fld = key.getKeyFldSet().iterator().next();
		Constant val = key.getKeyVal(fld);
		return Math.abs(val.hashCode() % PartitionMetaMgr.NUM_PARTITIONS);
	}
}
