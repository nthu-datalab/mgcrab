package org.vanilladb.dd.schedule.tpart;

import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

public class CostFunctionCalculator {
	private static final double BETA;
	private double[] partLoads = new double[TPartPartitioner.NUM_PARTITIONS];
	private double totalLoads = 0; // used to speed up the sum of partLoads
	public int crossEdgeCount;
	public static int ttt;
	static {
		String prop = System.getProperty(CostFunctionCalculator.class.getName()
				+ ".BETA");
		if (prop != null && !prop.isEmpty())
			BETA = Double.parseDouble(prop.trim());
		else
			BETA = 1;
	}

	public CostFunctionCalculator() {
		reset();
	}

	public void reset() {
		ttt += crossEdgeCount;
		for (int i = 0; i < partLoads.length; i++)
			partLoads[i] = 0;
		crossEdgeCount = 0;
		totalLoads = 0;
	}

	/**
	 * Update the cost with a node added or removed.
	 */
	private void updateCost(Node node, TGraph graph, boolean isAdd) {
		int coef = isAdd ? 1 : -1;

		partLoads[node.getPartId()] += (node.getWeight() * coef);
		totalLoads += (node.getWeight() * coef);

		if (node.getTask() == null || node.getTask().getReadSet() == null)
			return;

		// XXX Consider the edge weight
		for (RecordKey res : node.getTask().getReadSet()) {
			if (VanillaDdDb.partitionMetaMgr().isFullyReplicated(res))
				continue;
			if (graph.getResourcePosition(res).getPartId() != node.getPartId()) {
				crossEdgeCount += coef;
			}
		}
	}

	/**
	 * Incrementally calculate the cost after adding one node.
	 */
	public void updateAddNodeCost(Node newNode, TGraph graph) {
		updateCost(newNode, graph, true);
	}

	/**
	 * Incrementally calculate the cost after removing one node.
	 */
	public void updateRemoveNodeCost(Node removeNode, TGraph graph) {
		updateCost(removeNode, graph, false);
	}

	/**
	 * Calculate the cost with a node added. ! The real cost will not be updated
	 * by this method.
	 */
	public double calAddNodeCost(Node newNode, TGraph graph) {
		// calculate cross partition edge cost
		double edgeCost = 0;
		for(int part = 0; part < TPartPartitioner.NUM_PARTITIONS; part++){
			if(part != newNode.getPartId())
				edgeCost += newNode.getPartRecordCntArray()[part];
		}
		
		// calculate partition load cost
		double averageLoad = (totalLoads + newNode.getWeight())/TPartPartitioner.NUM_PARTITIONS;
		double loadCost = 0;
		
		for(int part = 0; part < TPartPartitioner.NUM_PARTITIONS; part++){
			if (part == newNode.getPartId())
				loadCost += Math.abs(partLoads[part] + newNode.getWeight() - averageLoad);
			else
				loadCost += Math.abs(partLoads[part]  - averageLoad);
		}
		
		return truncate(loadCost * (1 - BETA) + edgeCost * BETA, 4);
	}

	/**
	 * Calculate the cost with a node removed. ! The real cost will not be
	 * updated by this method.
	 */
	public double calRemoveNodeCost(Node removeNode, TGraph graph) {
		updateCost(removeNode, graph, false);
		double cost = getCost();
		updateCost(removeNode, graph, true);
		return cost;
	}
	
	/**
	 * Calculate the number of record for each partition as an array
	 */
	public int[] calPartitionRecordCount(Node node, TGraph graph){
		int[] array = new int[TPartPartitioner.NUM_PARTITIONS];
		
		for (RecordKey res : node.getTask().getReadSet()) {
			if (VanillaDdDb.partitionMetaMgr().isFullyReplicated(res))
				continue;
			
			array[graph.getResourcePosition(res).getPartId()]++;
		}
		
		return array;
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

	/*
	 * public void printCost() { double totalLoad = 0; for (Double d :
	 * partLoads) { totalLoad += d; } double loadBalance = 0; for (Double d :
	 * partLoads) loadBalance += Math.abs(d - totalLoad / partLoads.length);
	 * System.out.println("BETA: " + BETA); System.out.println("Edge: " +
	 * crossEdgeCount); double score = truncate(loadBalance * (1 - BETA) +
	 * crossEdgeCount BETA, 4); System.out.println("Score: " + score); }
	 */

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

	// public double getEdgeCost(TGraph graph) {
	// double edgeCost = 0;
	//
	// for (Edge edge : graph.getEdges()) {
	// if (edge.getSource().getPartId() == edge.getDestination()
	// .getPartId())
	// continue;
	// // if is sink node, skip this round
	// int sourceIdx = graph.getNodes().indexOf(edge.getSource());
	// int destIdx = graph.getNodes().indexOf(edge.getDestination());
	// if (sourceIdx == -1 || destIdx == -1)
	// continue;
	// edgeCost += destIdx - sourceIdx;
	// }
	//
	// return edgeCost;
	// }
	//
	// public int getEdgeCut(TGraph graph) {
	// int edgeCut = 0;
	// for (Edge edge : graph.getEdges()) {
	// if (edge.getSource().getPartId() == edge.getDestination()
	// .getPartId())
	// continue;
	// // if is sink node, skip this round
	// int sourceIdx = graph.getNodes().indexOf(edge.getSource());
	// int destIdx = graph.getNodes().indexOf(edge.getDestination());
	// if (sourceIdx == -1 || destIdx == -1)
	// continue;
	//
	// edgeCut++;
	// }
	//
	// return edgeCut;
	// }
	//
	// private double calEdgeWeight(Edge e, TGraph graph) {
	// int sourceId = graph.getNodes().indexOf(e.getSource());
	// int destId = graph.getNodes().indexOf(e.getDestination());
	// return sigmoidFunc(-2.5, 3, destId - sourceId);
	// }

	private static double sigmoidFunc(double a, double b, double x) {
		return 1 / (1 + Math.exp(-1 * (a * (x - b))));
	}
}
