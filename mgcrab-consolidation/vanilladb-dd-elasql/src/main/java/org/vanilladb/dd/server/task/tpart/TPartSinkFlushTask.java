package org.vanilladb.dd.server.task.tpart;

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.dd.schedule.tpart.sink.SinkFlushProcedure;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.sql.RecordVersion;

public class TPartSinkFlushTask extends TPartStoredProcedureTask {
	private SinkFlushProcedure sfp;
	private Set<RecordVersion> writeBackList = new HashSet<RecordVersion>();

	public TPartSinkFlushTask(SinkFlushProcedure sfp) {
		super(-1, -1, sfp);
		this.sfp = sfp;
		this.sfp.prepare(writeBackList);
	}

	public void writeBack(RecordKey key, long srcTxNum) {
		writeBackList.add(new RecordVersion(key, srcTxNum));
	}

	@Override
	public void run() {
		sfp.execute();
	}
}
