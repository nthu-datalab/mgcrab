package org.vanilladb.dd.junk;

import org.vanilladb.dd.sql.RecordKey;

public interface ReplicationAndPartitionMetaMgr {

	boolean isFullyReplicatedReadOnly(RecordKey key);

	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 * @return the partition ids
	 */
	int[] getPartition(RecordKey key);
}
