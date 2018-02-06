package org.vanilladb.dd.storage.metadata;

import org.vanilladb.dd.sql.RecordKey;

public abstract class PartitionMetaMgr {
	
	public final static int NUM_PARTITIONS;
	
	static {
		String prop = System.getProperty(PartitionMetaMgr.class.getName()
				+ ".NUM_PARTITIONS");
		NUM_PARTITIONS = prop == null ? 1 : Integer.valueOf(prop.trim());
	}
	
	// TODO: usage
	public abstract boolean isFullyReplicated(RecordKey key);

	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 * @return the partition id
	 */
	public abstract int getPartition(RecordKey key);
}
