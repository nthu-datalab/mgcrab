package org.vanilladb.dd.schedule.tpart;

public interface NodeInserter {

	/**
	 * Insert a new node to the graph.
	 * 
	 * @param graph
	 * @param node
	 */
	void insert(TGraph graph, Node node);
}
