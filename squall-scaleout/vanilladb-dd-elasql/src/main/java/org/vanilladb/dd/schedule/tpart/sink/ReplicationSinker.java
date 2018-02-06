package org.vanilladb.dd.schedule.tpart.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.vanilladb.dd.schedule.tpart.replication.ReplicatedEdge;
import org.vanilladb.dd.schedule.tpart.replication.ReplicatedNode;
import org.vanilladb.dd.schedule.tpart.replication.ReplicatedNode.Replica;
import org.vanilladb.dd.schedule.tpart.replication.ReplicatedTGraph;
import org.vanilladb.dd.schedule.tpart.replication.ReplicatedTPartPartitioner;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.tpart.TPartSinkFlushTask;
import org.vanilladb.dd.server.task.tpart.TPartSinkPushTask;
import org.vanilladb.dd.server.task.tpart.TPartStoredProcedureTask;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.sql.RecordVersion;

public class ReplicationSinker {

	private static long sinkProcedureTxNum = -1;
	private static int sinkId = 0;
	private int myId = VanillaDdDb.serverId();

	public ReplicationSinker() {
	}

	public Iterator<TPartStoredProcedureTask> sink(ReplicatedTGraph graph) {
		List<TPartStoredProcedureTask> plans;
		graph.addWriteBackEdge();
		plans = createSunkPlan(graph);

		graph.clearSinkNodeEdges();
		graph.removeSunkNodes();
		sinkId++;
		ReplicatedTPartPartitioner.costFuncCal.reset();
		return plans.iterator();
	}

	/**
	 * Note: only sink the task that will be executed in this machine to result
	 * set.
	 * 
	 * @param node
	 * @return
	 */
	private List<TPartStoredProcedureTask> createSunkPlan(ReplicatedTGraph graph) {
		// prepare for cache info

		// the version of record will read from remote site
		Set<RecordVersion> remoteFlags = new HashSet<RecordVersion>();

		// the version of record that will be write back to local storage
		Map<RecordKey, Long> writeBackFlags = new HashMap<RecordKey, Long>();

		// the txs that will perform read in this sink
		List<Long> readingTxs = new ArrayList<Long>();

		List<TPartStoredProcedureTask> localTasks = new LinkedList<TPartStoredProcedureTask>();
		TPartSinkPushTask sinkPushTask = new TPartSinkPushTask(
				new SinkPushProcedure(sinkProcedureTxNum, sinkId));
		sinkProcedureTxNum--;
		TPartSinkFlushTask sinkFlushTask = new TPartSinkFlushTask(
				new SinkFlushProcedure(sinkProcedureTxNum, sinkId));
		sinkProcedureTxNum--;

		localTasks.add(sinkPushTask);

		for (ReplicatedNode node : graph.getNodes()) {
			for (Entry<Integer, Replica> replicaEntry : node.replicaMap
					.entrySet()) {
				Replica replica = replicaEntry.getValue();
				boolean taskIsLocal = (replica.partId == myId);
				long txNum = node.getTxNum();
				// System.out.println("task " + txNum + " replica"
				// + replicaEntry.getKey() + " part:" + replica.partId);
				SunkPlan plan = null;
				if (taskIsLocal) {
					localTasks.add(node.getTask());
					plan = new SunkPlan(sinkId);
					node.getTask().setSunkPlan(plan);
					if (replica.readEdges.size() > 0)
						readingTxs.add(txNum);
				}

				// readings
				for (ReplicatedEdge e : replica.readEdges) {
					// System.out.println("replica" + replicaEntry.getKey()
					// + " read " + e.getResourceKey() + " from "
					// + e.getTarget().getTxNum());
					long srcTxn = e.getTarget().getTxNum();
					boolean isLocalResource;
					if (e.getTarget().isSinkNode()) {
						isLocalResource = isInMyPartition(e.getResourceKey());
					} else
						isLocalResource = e.getTarget().hasReplicaInPartition(
								myId);
					// System.out.println(e.getResourceKey() + " islocal?"
					// + isLocalResource);
					if (taskIsLocal) {
						plan.addReadingInfo(e.getResourceKey(), srcTxn);
						if (!isLocalResource)
							remoteFlags.add(new RecordVersion(e
									.getResourceKey(), srcTxn));
					} else if (isLocalResource && e.getTarget().isSinkNode()) {
						sinkPushTask.addPushingInfo(e.getResourceKey(),
								replica.partId);
					}
				}

				// writings
				if (taskIsLocal) {
					for (ReplicatedEdge e : replica.writeEdges) {
						// System.out.println("replica" + replicaEntry.getKey()
						// + " write " + e.getResourceKey());
						Replica targetReplica = e.getTarget().replicaMap.get(e
								.getReplicaId());
						// push to remote if dest. node not in local
						if (targetReplica.partId != myId)
							plan.addPushingInfo(e.getResourceKey(),
									targetReplica.partId);
					}
				}

				// write back
				for (ReplicatedEdge e : replica.writeBackEdges) {

					int targetServerId = e.getTarget().replicaMap.get(0).partId;
					// System.out.println("replica" + replicaEntry.getKey()
					// + " writeback " + e.getResourceKey() + " to sink"
					// + targetServerId);
					RecordKey k = e.getResourceKey();
					// push the data if write back to remote
					if (taskIsLocal) {
						if (targetServerId == myId) {
							sinkFlushTask.writeBack(k, txNum);
							writeBackFlags.put(k, txNum);
						} else {
							plan.addPushingInfo(k, targetServerId);
						}
					} else {
						if (targetServerId == myId) {
							sinkFlushTask.writeBack(k, txNum);
							remoteFlags.add(new RecordVersion(k, txNum));
							writeBackFlags.put(k, txNum);
						}
					}

				}
			}
			node.setSunk(true);
		}

		if (sinkPushTask.hasToPush())
			readingTxs.add((long) -1);

		VanillaDdDb.tPartCacheMgr().createSinkCachePool(sinkId,
				new ArrayList<RecordVersion>(remoteFlags), writeBackFlags,
				readingTxs);
		localTasks.add(sinkFlushTask);
		return localTasks;
	}

	private boolean isInMyPartition(RecordKey k) {
		int[] homeSinks = VanillaDdDb.replicationAndPartitionMgr()
				.getPartition(k);
		for (int s : homeSinks)
			if (s == myId)
				return true;
		return false;
	}
}
