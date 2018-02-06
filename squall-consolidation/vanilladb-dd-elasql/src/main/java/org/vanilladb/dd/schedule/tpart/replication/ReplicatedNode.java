package org.vanilladb.dd.schedule.tpart.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.vanilladb.dd.server.task.tpart.TPartStoredProcedureTask;

public class ReplicatedNode {
	public Map<Integer, Replica> replicaMap;
	private TPartStoredProcedureTask task;
	private boolean sunk;
	private int[] replicaPartitionIds;

	public ReplicatedNode(TPartStoredProcedureTask task) {
		this.task = task;
		replicaMap = new HashMap<Integer, Replica>();
		replicaPartitionIds = new int[ReplicatedTPartPartitioner.NUM_REPLICATIONS];
		for (int i = 0; i < replicaPartitionIds.length; i++)
			replicaPartitionIds[i] = Integer.MIN_VALUE;
	}

	public TPartStoredProcedureTask getTask() {
		return task;
	}

	public double getWeight() {
		return task.getWeight();
	}

	public boolean hasSunk() {
		return sunk;
	}

	public void setSunk(boolean sunk) {
		this.sunk = sunk;
	}

	public boolean isSinkNode() {
		return task == null;
	}

	public long getTxNum() {
		if (isSinkNode())
			return -1;
		else
			return task.getTxNum();
	}

	public void setPartId(int replicaId, int partId) {
		Replica r = replicaMap.get(replicaId);
		if (r == null) {
			r = new Replica();
			replicaMap.put(replicaId, r);
		}
		r.partId = partId;
		replicaPartitionIds[replicaId] = partId;
	}

	public boolean hasReplicaInPartition(int partId) {
		for (int p : replicaPartitionIds)
			if (p == partId)
				return true;
		return false;
	}

	public int getReplicaId(int partId) {
		for (int i = 0; i < replicaPartitionIds.length; i++) {
			if (replicaPartitionIds[i] == partId)
				return i;
		}
		return Integer.MIN_VALUE;
	}

	public boolean isReadOnly() {
		return task.isReadOnly();
	}

	public class Replica {
		public List<ReplicatedEdge> readEdges = new ArrayList<ReplicatedEdge>();
		public List<ReplicatedEdge> writeEdges = new ArrayList<ReplicatedEdge>();
		public List<ReplicatedEdge> writeBackEdges = new ArrayList<ReplicatedEdge>();
		public int partId;

		public Replica() {

		}
	}

	// @Override
	// public String toString() {
	// return "[Node] Txn-id: " + getTask().getTxNum() + ", " + "read-edges: "
	// + readEdges + ", write-edges: " + writeEdges + ", weight: "
	// + task.getWeight();
	// }

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj.getClass() != ReplicatedNode.class)
			return false;
		ReplicatedNode n = (ReplicatedNode) obj;
		return (n.task.getTxNum() == this.task.getTxNum());
	}
}
