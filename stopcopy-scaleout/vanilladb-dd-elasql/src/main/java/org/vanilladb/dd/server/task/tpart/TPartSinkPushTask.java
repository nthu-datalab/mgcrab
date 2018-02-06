package org.vanilladb.dd.server.task.tpart;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vanilladb.dd.schedule.tpart.sink.PushInfo;
import org.vanilladb.dd.schedule.tpart.sink.SinkPushProcedure;
import org.vanilladb.dd.sql.RecordKey;

public class TPartSinkPushTask extends TPartStoredProcedureTask {
	private Map<Integer, Set<PushInfo>> pushInfoMap = new HashMap<Integer, Set<PushInfo>>();
	private Map<RecordKey, Set<Long>> readMap = new HashMap<RecordKey, Set<Long>>();
	private SinkPushProcedure sp;

	public TPartSinkPushTask(SinkPushProcedure spp) {
		super(-1, -1, spp);
		sp = spp;
		sp.prepare(pushInfoMap, readMap);
	}

	public void addPushingInfo(RecordKey key, int destNodeId, long srcTxNum,
			long destTxNum) {
		Set<PushInfo> pushInfos = pushInfoMap.get(destNodeId);
		if (pushInfos == null) {
			pushInfos = new HashSet<PushInfo>();
			pushInfoMap.put(destNodeId, pushInfos);
		}
		pushInfos.add(new PushInfo(destTxNum, destNodeId, key));
	}

	/**
	 * add the new destination transaction number of a record key into the read
	 * map.
	 */
	public void addReadingInfo(RecordKey key, long destTxNum) {
		if (readMap.get(key) == null)
			readMap.put(key, new HashSet<Long>());
		readMap.get(key).add(destTxNum);
	}

	public boolean hasToPush() {
		return pushInfoMap.size() > 0;
	}

	@Override
	public void run() {
		sp.execute();
	}
}
