package netdb.software.benchmark.tpcc.vanilladddb.metadata;

import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.rte.MicroBenchmarkTxnExecutor;

import org.vanilladb.dd.junk.ReplicationAndPartitionMetaMgr;
import org.vanilladb.dd.schedule.tpart.replication.ReplicatedTPartPartitioner;
import org.vanilladb.dd.sql.RecordKey;

public class RepMicroBenchPartitionMetaMgr implements
		ReplicationAndPartitionMetaMgr {
	public static int[] iid_boundaries;

	static {
		String prop = System.getProperty(MicroBenchmarkTxnExecutor.class
				.getName() + ".PARTITION_NUM");
		int partition_num = (prop == null ? 3 : Integer.parseInt(prop.trim()));
		String[] sd = System.getProperty(
				MicroBenchmarkTxnExecutor.class.getName()
						+ ".SKEWNESS_DISTRIBUTION").split(",");
		if (sd.length != partition_num)
			throw new RuntimeException(
					"the skewness information is not complete");

		String s = System.getProperty(TpccConstants.class.getName()
				+ ".NUM_ITEMS");
		int item_num = Integer.parseInt(s);

		double[] SKEWNESS_DISTRIBUTION = new double[partition_num];
		for (int i = 0; i < partition_num; i++)
			SKEWNESS_DISTRIBUTION[i] = Double.parseDouble(sd[i].trim());
		iid_boundaries = new int[partition_num + 1];
		for (int i = 1; i < partition_num + 1; i++) {
			iid_boundaries[i] = iid_boundaries[i - 1]
					+ (int) (SKEWNESS_DISTRIBUTION[i - 1] * item_num);
		}
		iid_boundaries[partition_num] = item_num;

	}

	public RepMicroBenchPartitionMetaMgr() {
	}

	public int[] getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */
		int primaryPartId = -1;
		int iid = (Integer) key.getKeyVal("i_id").asJavaVal();
		for (int i = 0; i < ReplicatedTPartPartitioner.NUM_PARTITIONS; ++i) {
			if (iid < iid_boundaries[i + 1]) {
				primaryPartId = i;
				break;
			}
		}
		if (primaryPartId == -1)
			primaryPartId = ReplicatedTPartPartitioner.NUM_PARTITIONS - 1;

		int[] partIds = new int[ReplicatedTPartPartitioner.NUM_REPLICATIONS];
		for (int i = 0; i < partIds.length; i++) {
			partIds[i] = (primaryPartId + i)
					% ReplicatedTPartPartitioner.NUM_PARTITIONS;
		}
		return partIds;

		// return iid % TPartPartitioner.NUM_PARTITIONS;
	}

	@Override
	public boolean isFullyReplicatedReadOnly(RecordKey key) {
		return false;
	}
}
