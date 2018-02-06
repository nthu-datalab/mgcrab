package org.vanilladb.dd.schedule.tpart.replication;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.vanilladb.dd.junk.ReplicationAndPartitionMetaMgr;
import org.vanilladb.dd.schedule.tpart.replication.ReplicatedNode.Replica;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

public class ReplicatedTGraph {
	public static final boolean READ_REMOTE_FROM_PRIMARY = true;

	private List<ReplicatedNode> nodes = new LinkedList<ReplicatedNode>();
	private Map<RecordKey, ReplicatedNode> resPos = new HashMap<RecordKey, ReplicatedNode>();
	private ReplicatedNode[] sinkNodes;
	private ReplicationAndPartitionMetaMgr parMeta;

	public ReplicatedTGraph() {
		sinkNodes = new ReplicatedNode[ReplicatedTPartPartitioner.NUM_PARTITIONS];
		for (int i = 0; i < sinkNodes.length; i++) {
			ReplicatedNode node = new ReplicatedNode(null);
			node.setPartId(0, i);
			sinkNodes[i] = node;
		}
		parMeta = VanillaDdDb.replicationAndPartitionMgr();
	}

	public boolean hasResource(int partId, RecordKey res) {
		if (resPos.containsKey(res)) {
			ReplicatedNode node = resPos.get(res);
			return node.hasReplicaInPartition(partId);
		}
		return isHomePartition(partId, res);
	}

	public boolean isHomePartition(int partId, RecordKey res) {
		int[] pos = parMeta.getPartition(res);
		for (int i = 0; i < pos.length; i++)
			if (pos[i] == partId) {
				return true;
			}
		return false;
	}

	/**
	 * Insert the new node into the t-graph.
	 * 
	 * @param node
	 */
	public void insertNode(ReplicatedNode node) {
		if (node.getTask() == null)
			return;

		nodes.add(node);
		if (node.getTask().getReadSet() != null) {
			for (Entry<Integer, Replica> e : node.replicaMap.entrySet()) {
				Replica replica = e.getValue();
				for (RecordKey res : node.getTask().getReadSet()) {
					ReplicatedNode targetNode = chooseTargetNode(node, res,
							replica.partId);
					if (targetNode.isSinkNode()) {
						replica.readEdges.add(new ReplicatedEdge(targetNode,
								res, 0));
						targetNode.replicaMap.get(0).writeEdges
								.add(new ReplicatedEdge(node, res, e.getKey()));
					} else {
						// choose the replica in local partition if possible
						if (targetNode.hasReplicaInPartition(replica.partId)) {
							int targetReplicaId = targetNode
									.getReplicaId(replica.partId);
							replica.readEdges.add(new ReplicatedEdge(
									targetNode, res, targetReplicaId));
							targetNode.replicaMap.get(targetReplicaId).writeEdges
									.add(new ReplicatedEdge(node, res, e
											.getKey()));
						} else if (READ_REMOTE_FROM_PRIMARY) {
							int targetReplicaId = 0;
							replica.readEdges.add(new ReplicatedEdge(
									targetNode, res, targetReplicaId));
							targetNode.replicaMap.get(targetReplicaId).writeEdges
									.add(new ReplicatedEdge(node, res, e
											.getKey()));
						} else {// ask all src replicas to send the readings
							replica.readEdges.add(new ReplicatedEdge(
									targetNode, res, 0));
							for (int i = 0; i < ReplicatedTPartPartitioner.NUM_REPLICATIONS; i++) {
								targetNode.replicaMap.get(i).writeEdges
										.add(new ReplicatedEdge(node, res, e
												.getKey()));
							}
						}
					}
				}
			}
		}
		// update the resource position
		if (node.getTask().getWriteSet() != null)
			for (RecordKey res : node.getTask().getWriteSet())
				resPos.put(res, node);
	}

	private ReplicatedNode chooseTargetNode(ReplicatedNode node, RecordKey res,
			int partId) {
		ReplicatedNode targetNode;
		if (parMeta.isFullyReplicatedReadOnly(res))
			targetNode = sinkNodes[partId];
		else if ((targetNode = getLatestUpdatedNode(res)) != null) {
			targetNode = getLatestUpdatedNode(res);
		} else {// read from sink
			ReplicatedNode sinks[] = getHomeSinkNodes(res);
			for (int i = 0; i < sinks.length; i++) {
				// choose local sink if possible
				if (sinks[i].replicaMap.get(0).partId == partId) {
					targetNode = sinks[i];
					break;
				}
			}
			// otherwise, choose the primary sink
			if (targetNode == null)
				targetNode = sinks[0];
		}
		return targetNode;
	}

	public static final int WRITE_BACK_DETERMINISTICALLY = 1,
			WRITE_BACK_ALL = 2, WRITE_BACK_BY_PRIMARY = 3;
	public static int WRTIE_BACK_POLICY = 1;

	/**
	 * Write back all modified data records following the specified write-back
	 * policy.
	 */
	public void addWriteBackEdge() {
		Iterator<Entry<RecordKey, ReplicatedNode>> resPosItr = resPos
				.entrySet().iterator();
		while (resPosItr.hasNext()) {
			Entry<RecordKey, ReplicatedNode> entry = resPosItr.next();
			RecordKey res = entry.getKey();
			ReplicatedNode node = entry.getValue();
			ReplicatedNode[] homeSinkNode = getHomeSinkNodes(res);
			if (WRTIE_BACK_POLICY == WRITE_BACK_DETERMINISTICALLY) {
				for (int i = 0; i < homeSinkNode.length; i++) {
					node.replicaMap.get(i).writeBackEdges
							.add(new ReplicatedEdge(homeSinkNode[i], res, 0));
				}
			} else if (WRTIE_BACK_POLICY == WRITE_BACK_ALL) {
				for (int i = 0; i < ReplicatedTPartPartitioner.NUM_REPLICATIONS; i++) {
					for (int j = 0; j < ReplicatedTPartPartitioner.NUM_REPLICATIONS; j++) {
						node.replicaMap.get(i).writeBackEdges
								.add(new ReplicatedEdge(homeSinkNode[j], res, 0));
					}
				}
			} else if (WRTIE_BACK_POLICY == WRITE_BACK_BY_PRIMARY) {
				for (int j = 0; j < ReplicatedTPartPartitioner.NUM_REPLICATIONS; j++) {
					node.replicaMap.get(0).writeBackEdges
							.add(new ReplicatedEdge(homeSinkNode[j], res, 0));
				}
			}
		}
		resPos.clear();
	}

	public void clearSinkNodeEdges() {
		for (int i = 0; i < sinkNodes.length; i++)
			sinkNodes[i].replicaMap.get(0).writeEdges.clear();
	}

	public void removeSunkNodes() {
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
	public ReplicatedNode getLatestUpdatedNode(RecordKey res) {
		return resPos.get(res);
	}

	public ReplicatedNode[] getHomeSinkNodes(RecordKey res) {
		int hsi[] = parMeta.getPartition(res);
		ReplicatedNode hsn[] = new ReplicatedNode[ReplicatedTPartPartitioner.NUM_REPLICATIONS];
		for (int i = 0; i < hsi.length; i++) {
			ReplicatedNode n = sinkNodes[hsi[i]];
			hsn[i] = n;
		}
		return hsn;
	}

	public List<ReplicatedNode> getNodes() {
		return nodes;
	}
}
