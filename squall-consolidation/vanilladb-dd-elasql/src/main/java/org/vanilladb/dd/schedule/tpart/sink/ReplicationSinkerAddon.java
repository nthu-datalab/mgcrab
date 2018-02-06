package org.vanilladb.dd.schedule.tpart.sink;

import java.util.Iterator;

import org.vanilladb.dd.schedule.tpart.TGraph;
import org.vanilladb.dd.server.task.tpart.TPartStoredProcedureTask;

public class ReplicationSinkerAddon extends SinkerAddon {

	public ReplicationSinkerAddon(Sinker sinker) {
		super(sinker);
	}

	@Override
	public Iterator<TPartStoredProcedureTask> sink(TGraph graph) {

		// Iterator<SunkPlan> plans = sinker.sink(graph);

		// TODO do replication to plans here

		return null;
	}

	@Override
	public Iterator<TPartStoredProcedureTask> sink(TGraph graph, long txNum) {
		// TODO Auto-generated method stub
		return null;
	}

}
