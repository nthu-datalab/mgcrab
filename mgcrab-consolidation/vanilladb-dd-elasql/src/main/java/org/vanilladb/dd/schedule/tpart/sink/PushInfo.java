package org.vanilladb.dd.schedule.tpart.sink;

import org.vanilladb.dd.sql.RecordKey;

public class PushInfo {
	private long destTxNum;
	private int serverId;
	private RecordKey record;

	public PushInfo(long destTxNum, int serverId, RecordKey record) {
		this.destTxNum = destTxNum;
		this.serverId = serverId;
		this.record = record;
	}

	public long getDestTxNum() {
		return destTxNum;
	}

	public void setDestTxNum(long destTxNum) {
		this.destTxNum = destTxNum;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public RecordKey getRecord() {
		return record;
	}

	public void setRecord(RecordKey record) {
		this.record = record;
	}
}
