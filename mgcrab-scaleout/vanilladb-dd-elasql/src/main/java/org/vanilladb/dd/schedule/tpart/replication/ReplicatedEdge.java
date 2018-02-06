package org.vanilladb.dd.schedule.tpart.replication;

import org.vanilladb.dd.sql.RecordKey;

public class ReplicatedEdge {
	private ReplicatedNode target;
	private RecordKey resource;
	private int replicaId;

	public ReplicatedEdge(ReplicatedNode target, RecordKey res, int replicaId) {
		this.target = target;
		this.resource = res;
		this.replicaId = replicaId;
	}

	public ReplicatedNode getTarget() {
		return target;
	}

	public RecordKey getResourceKey() {
		return resource;
	}

	public int getReplicaId() {
		return replicaId;
	}
}
