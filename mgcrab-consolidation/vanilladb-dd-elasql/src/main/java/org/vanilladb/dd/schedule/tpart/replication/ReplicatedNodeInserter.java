package org.vanilladb.dd.schedule.tpart.replication;


public class ReplicatedNodeInserter {

	public ReplicatedNodeInserter() {
	}

	/**
	 * Insert this node to the partition that will result in minimal cost.
	 */
	public void insert(ReplicatedTGraph graph, ReplicatedNode node) {
		if (node.isReadOnly())
			insertReadOnlyNode(graph, node);
		else
			insertReadWriteNode(graph, node);
	}

	private void insertReadOnlyNode(ReplicatedTGraph graph, ReplicatedNode node) {
		double minCost = Double.MAX_VALUE;
		int partId = 0;
		// choose the partition with least cost
		for (int p = 0; p < ReplicatedTPartPartitioner.NUM_PARTITIONS; p++) {
			double cost = ReplicatedTPartPartitioner.costFuncCal
					.calAddNodeCost(node, graph, p);
			if (cost < minCost) {
				minCost = cost;
				partId = p;
			}
		}
		node.setPartId(0, partId);
		ReplicatedTPartPartitioner.costFuncCal.updateAddNodeCost(node, graph,
				partId);
		graph.insertNode(node);
	}

	private void insertReadWriteNode(ReplicatedTGraph graph, ReplicatedNode node) {
		double[] costPerPartition = new double[ReplicatedTPartPartitioner.NUM_PARTITIONS];
		for (int p = 0; p < ReplicatedTPartPartitioner.NUM_PARTITIONS; p++)
			costPerPartition[p] = ReplicatedTPartPartitioner.costFuncCal
					.calAddNodeCost(node, graph, p);
		// choose the partition with k(=NUM_REPLICAS) least cost
		int replica = 0;
		while (replica < ReplicatedTPartPartitioner.NUM_REPLICATIONS) {
			double minCost = Double.MAX_VALUE;
			int partId = 0;
			for (int p = 0; p < ReplicatedTPartPartitioner.NUM_PARTITIONS; p++)
				if (costPerPartition[p] < minCost) {
					minCost = costPerPartition[p];
					partId = p;
				}

			node.setPartId(replica, partId);
			ReplicatedTPartPartitioner.costFuncCal.updateAddNodeCost(node,
					graph, partId);
			costPerPartition[partId] = Double.MAX_VALUE;
			replica++;
		}
		graph.insertNode(node);
	}
}