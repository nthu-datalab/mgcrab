package org.vanilladb.dd.schedule.tpart.sink;


public class ReorderingSinker {// extends SinkerDecorator {

	public static double THRESHOLD = 0.005;
	private static double noReorder = 0;
	private static double reorder = 0;
	private static double sinkCount = 0;

	// public ReorderingSinker(Sinker sinker) {
	// super(sinker);
	// }
	//
	// @Override
	// public Iterator<TPartStoredProcedureTask> sink(TGraph graph) {
	//
	// /*
	// * For each cross partition edge, separate the source and destination
	// * node as far as possible.
	// */
	// // System.out.println("Before Reordering score: "
	// // + TPartPartitioner.costFuncCal.getEdgeCost(graph));
	// sinkCount++;
	// noReorder += TPartPartitioner.costFuncCal.getEdgeCost(graph);
	//
	// long time = System.nanoTime();
	// for (Edge edge : graph.getEdges()) {
	//
	// Node source = edge.getSource();
	// Node dest = edge.getDestination();
	//
	// // check if this edge is local edge, skip it
	// if (source.getPartId() == dest.getPartId())
	// continue;
	//
	// int sourceIdx = graph.getNodes().indexOf(source);
	// int destIdx = graph.getNodes().indexOf(dest);
	//
	// if (destIdx == -1)
	// continue;
	// // System.out.println("Part " + source.getPartId() + "->"
	// // + dest.getPartId());
	// // System.out.println("***[Txn" + source.getTxNum() + " -> Txn "
	// // + dest.getTxNum() + "]source idx:"
	// // + graph.getNodes().indexOf(source) + ", dest idx:"
	// // + graph.getNodes().indexOf(dest));
	//
	// // move down the source node
	// int i;
	// for (i = sourceIdx - 1; i >= 0; i--) {
	// if (!checkSwappable(graph.getNodes().get(i), source, graph)) {
	// break;
	// }
	// }
	//
	// // move up the destination node
	// for (i = destIdx + 1; i < graph.getNodes().size(); i++) {
	// if (!checkSwappable(dest, graph.getNodes().get(i), graph)) {
	// break;
	// }
	// }
	//
	// // System.out.println("source idx:" +
	// // graph.getNodes().indexOf(source)
	// // + ", dest idx:" + graph.getNodes().indexOf(dest));
	//
	// }
	// // System.out.println("It takes " + (System.nanoTime() - time)
	// // + " ns to do the Reordering !");
	// // System.out.println("After Reordering score: "
	// // + TPartPartitioner.costFuncCal.getEdgeCost(graph));
	// reorder += TPartPartitioner.costFuncCal.getEdgeCost(graph);
	//
	// System.out.println("Average NoReorder: " + noReorder / sinkCount
	// + ", WithReorder: " + reorder / sinkCount);
	//
	// return sinker.sink(graph);
	// }
	//
	// private boolean checkSwappable(Node lower, Node upper, TGraph graph) {
	// boolean result = true;
	//
	// for (Edge edge1 : lower.getWriteEdges()) {
	// for (Edge edge2 : upper.getWriteEdges())
	// if (edge1.getResourceKey().equals(edge2.getResourceKey()))
	// result = false;
	// for (Edge edge2 : upper.getReadEdges())
	// if (edge1.getResourceKey().equals(edge2.getResourceKey()))
	// result = false;
	// }
	//
	// for (Edge edge1 : lower.getReadEdges()) {
	// for (Edge edge2 : upper.getWriteEdges())
	// if (edge1.getResourceKey().equals(edge2.getResourceKey()))
	// result = false;
	// }
	//
	// if (result) {
	// int lowerIdx = graph.getNodes().indexOf(lower);
	// int upperIdx = graph.getNodes().indexOf(upper);
	// double costBeforeSwap = TPartPartitioner.costFuncCal
	// .getEdgeCost(graph);
	// swap(lowerIdx, upperIdx, graph);
	// double costAfterSwap = TPartPartitioner.costFuncCal
	// .getEdgeCost(graph);
	// if (costBeforeSwap <= costAfterSwap + THRESHOLD) {
	// // System.out.println("\tbefore: " + costBeforeSwap +
	// // ", after: "
	// // + costAfterSwap);
	// result = false;
	// swap(upperIdx, lowerIdx, graph);
	// }
	//
	// }
	//
	// return result;
	// }
	//
	// private void swap(int idx1, int idx2, TGraph graph) {
	// Node node1 = graph.getNodes().get(idx1);
	// Node node2 = graph.getNodes().get(idx2);
	// graph.getNodes().set(idx2, node1);
	// graph.getNodes().set(idx1, node2);
	// }
	//
	// @Override
	// public Iterator<TPartStoredProcedureTask> sink(TGraph graph, long txNum)
	// {
	// return null;
	// }

}
