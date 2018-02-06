package org.vanilladb.dd.schedule.tpart;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.vanilladb.core.util.Timers;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class TGraph {
	private List<Node> nodes = new LinkedList<Node>();
	private Map<RecordKey, Node> resPos = new HashMap<RecordKey, Node>();
	private Node[] sinkNodes;
	private PartitionMetaMgr parMeta;

	public TGraph() {
		sinkNodes = new Node[TPartPartitioner.NUM_PARTITIONS];
		for (int i = 0; i < sinkNodes.length; i++) {
			Node node = new Node(null);
			node.setPartId(i);
			sinkNodes[i] = node;
		}
		parMeta = VanillaDdDb.partitionMetaMgr();
	}

	/**
	 * Insert the new node into the t-graph.
	 * 
	 * @param node
	 */
	public void insertNode(Node node) {
		if (node.getTask() == null)
			return;

		nodes.add(node);

		if (node.getTask().getReadSet() != null)
			// create a read edge to the latest txn that writes that resource
			for (RecordKey res : node.getTask().getReadSet()) {
				Node targetNode;
				if (parMeta.isFullyReplicated(res))
					targetNode = sinkNodes[node.getPartId()];
				else
					targetNode = getResourcePosition(res);
				node.addReadEdges(new Edge(targetNode, res));
				targetNode.addWriteEdges(new Edge(node, res));
			}

		if (node.getTask().getWriteSet() != null)
			// update the resource position
			for (RecordKey res : node.getTask().getWriteSet())
				resPos.put(res, node);
	}

	/**
	 * Write back all modified data records to their original partitions.
	 */
	public void addWriteBackEdge() {
		// XXX should implement different write back strategy
		Iterator<Entry<RecordKey, Node>> resPosItr = resPos.entrySet()
				.iterator();
		while (resPosItr.hasNext()) {
			Entry<RecordKey, Node> entry = resPosItr.next();
			if (entry.getValue().getTask() != null) {
				RecordKey res = entry.getKey();
				Node node = entry.getValue();
				node.addWriteBackEdges(new Edge(sinkNodes[parMeta
						.getPartition(res)], res));
			}
		}
		resPos.clear();
	}

	public void clearSinkNodeEdges() {
		for (int i = 0; i < sinkNodes.length; i++)
			sinkNodes[i].getWriteEdges().clear();
	}

	public void removeSunkNodes() {
		// Iterator<Node> iter = nodes.iterator();
		// while (iter.hasNext()) {
		// Node n = iter.next();
		// if (n.hasSunk())
		// iter.remove();
		// }
		nodes.clear();
	}

	/**
	 * Get the node that produce the latest version of specified resource.
	 * 
	 * @param
	 * @return The desired node. If the resource has not been created a new
	 *         version since last sinking, the partition that own the resource
	 *         will be return in a Node format.
	 */
	public Node getResourcePosition(RecordKey res) {
		if (resPos.containsKey(res))
			return resPos.get(res);
		return sinkNodes[parMeta.getPartition(res)];
	}

	public List<Node> getNodes() {
		return nodes;
	}
}
