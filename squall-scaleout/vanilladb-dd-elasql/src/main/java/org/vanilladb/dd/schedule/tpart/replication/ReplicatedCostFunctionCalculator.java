package org.vanilladb.dd.schedule.tpart.replication;

import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

public class ReplicatedCostFunctionCalculator {
	private static final double BETA;
	private double[] partLoads = new double[ReplicatedTPartPartitioner.NUM_PARTITIONS];
	private int crossEdgeCount;

	static {
		String prop = System.getProperty(ReplicatedCostFunctionCalculator.class
				.getName() + ".BETA");
		if (prop != null && !prop.isEmpty())
			BETA = Double.parseDouble(prop.trim());
		else
			BETA = 1;
	}

	public ReplicatedCostFunctionCalculator() {
		reset();
	}

	public void reset() {
		for (int i = 0; i < partLoads.length; i++)
			partLoads[i] = 0;
		crossEdgeCount = 0;
	}

	/**
	 * Update the cost with a node added or removed.
	 */
	private void updateCost(ReplicatedNode node, ReplicatedTGraph graph,
			int partId, boolean isAdd) {
		if (node.getTask() == null || node.getTask().getReadSet() == null)
			return;

		// compute the cost of inserting this node into this partition
		double myCrossEdgeCost = 0;
		for (RecordKey res : node.getTask().getReadSet()) {
			if (VanillaDdDb.partitionMetaMgr().isFullyReplicated(res))
				continue;
			if (!graph.hasResource(partId, res)) {
				// XXX consider edge weight
				myCrossEdgeCost++;
			}
		}

		// choose the partitions of blind write tx to be the original partition
		if (node.getTask().getReadSet().length == 0
				&& node.getTask().getWriteSet() != null) {
			for (RecordKey res : node.getTask().getWriteSet()) {
				if (!graph.isHomePartition(partId, res)) {
					// XXX consider edge weight
					myCrossEdgeCost++;
				}
			}
		}
		int coef = isAdd ? 1 : -1;

		partLoads[partId] += (node.getWeight() * coef);
		crossEdgeCount += (myCrossEdgeCost * coef);
	}

	/**
	 * Incrementally calculate the cost after adding one node.
	 */
	public void updateAddNodeCost(ReplicatedNode newNode,
			ReplicatedTGraph graph, int partId) {
		updateCost(newNode, graph, partId, true);
	}

	/**
	 * Incrementally calculate the cost after removing one node.
	 */
	public void updateRemoveNodeCost(ReplicatedNode removeNode,
			ReplicatedTGraph graph, int partId) {
		updateCost(removeNode, graph, partId, false);
	}

	/**
	 * Calculate the cost with a node added. ! The real cost will not be updated
	 * by this method.
	 */
	public double calAddNodeCost(ReplicatedNode newNode,
			ReplicatedTGraph graph, int partId) {
		if (newNode.getTask() == null || newNode.getTask().getReadSet() == null)
			return 0;

		// compute the cost of inserting this node into this partition
		double cost = 0, myCrossEdgeCost = 0;
		for (RecordKey res : newNode.getTask().getReadSet()) {
			if (VanillaDdDb.partitionMetaMgr().isFullyReplicated(res))
				continue;
			if (!graph.hasResource(partId, res)) {
				// XXX consider edge weight
				myCrossEdgeCost++;

			}
		}

		// choose the partitions of blind write tx to be the original partition
		if (newNode.getTask().getReadSet().length == 0
				&& newNode.getTask().getWriteSet() != null) {
			for (RecordKey res : newNode.getTask().getWriteSet()) {
				if (!graph.isHomePartition(partId, res)) {
					// XXX consider edge weight
					myCrossEdgeCost++;
				}
			}
		}

		crossEdgeCount += myCrossEdgeCost;
		partLoads[partId] += newNode.getWeight();
		cost = getCost();

		// restore the current cost
		partLoads[partId] -= newNode.getWeight();
		crossEdgeCount -= myCrossEdgeCost;
		return cost;
	}

	/**
	 * Calculate the cost with a node removed. ! The real cost will not be
	 * updated by this method.
	 */
	public double calRemoveNodeCost(ReplicatedNode removeNode,
			ReplicatedTGraph graph, int partId) {
		if (removeNode.getTask() == null
				|| removeNode.getTask().getReadSet() == null)
			return 0;

		// compute the cost of inserting this node into this partition
		double cost = 0, myCrossEdgeCost = 0;
		for (RecordKey res : removeNode.getTask().getReadSet()) {
			if (VanillaDdDb.partitionMetaMgr().isFullyReplicated(res))
				continue;
			if (!graph.hasResource(partId, res)) {
				// XXX consider edge weight
				myCrossEdgeCost++;

			}
		}

		// choose the partitions of blind write tx to be the original partition
		if (removeNode.getTask().getReadSet().length == 0
				&& removeNode.getTask().getWriteSet() != null) {
			for (RecordKey res : removeNode.getTask().getWriteSet()) {
				if (!graph.isHomePartition(partId, res)) {
					// XXX consider edge weight
					myCrossEdgeCost++;
				}
			}
		}

		crossEdgeCount -= myCrossEdgeCost;
		partLoads[partId] -= removeNode.getWeight();
		cost = getCost();

		// restore the current cost
		partLoads[partId] += removeNode.getWeight();
		crossEdgeCount += myCrossEdgeCost;
		return cost;
	}

	/**
	 * Get the cost of current cost function state.
	 * 
	 * @return The cost of current cost function state. TODO alpha, beta
	 */
	public double getCost() {
		double totalLoad = 0;
		for (Double d : partLoads) {
			totalLoad += d;
		}
		double loadBalance = 0;
		for (Double d : partLoads)
			loadBalance += Math.abs(d - totalLoad / partLoads.length);
		return truncate(loadBalance * (1 - BETA) + crossEdgeCount * BETA, 4);
	}

	private double truncate(double number, int precision) {
		double prec = Math.pow(10, precision);
		int integerPart = (int) number;
		double fractionalPart = number - integerPart;
		fractionalPart *= prec;
		int fractPart = (int) fractionalPart;
		fractionalPart = (double) (integerPart) + (double) (fractPart) / prec;
		return fractionalPart;
	}

	public double[] getPartLoads() {
		return partLoads;
	}
}
