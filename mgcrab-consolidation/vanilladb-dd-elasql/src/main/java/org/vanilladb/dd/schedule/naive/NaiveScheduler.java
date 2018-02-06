package org.vanilladb.dd.schedule.naive;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.DdStoredProcedureFactory;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.server.task.StoredProcedureTask;
import org.vanilladb.dd.server.task.naive.NaiveStoredProcedureTask;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public class NaiveScheduler implements Scheduler {
	private static final String FACTORY_CLASS;

	private DdStoredProcedureFactory factory;

	static {
		String prop = System.getProperty(NaiveScheduler.class.getName()
				+ ".FACTORY_CLASS");

		if (prop != null && !prop.isEmpty())
			FACTORY_CLASS = prop.trim();
		else
			throw new RuntimeException("Factory property is empty");
	}

	public NaiveScheduler() {
		Class<?> c;
		try {
			c = Class.forName(FACTORY_CLASS);
			if (c != null)
				factory = (DdStoredProcedureFactory) c.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Factory doesn't exist, or can not be access");
		}
	}

	public void schedule(StoredProcedureCall... calls) {
		for (int i = 0; i < calls.length; i++) {
			StoredProcedureCall call = calls[i];

			// log request
			DdRecoveryMgr.logRequest(call);

			if (call.isNoOpStoredProcCall())
				continue;

			DdStoredProcedure sp = factory.getStoredProcedure(call.getPid(),
					call.getTxNum());
			sp.prepare(call.getPars());

			StoredProcedureTask spt = new NaiveStoredProcedureTask(
					call.getClientId(), call.getRteId(), call.getTxNum(), sp);

			// perform C2PL
			// TODO: Fix it
			spt.lockConservatively();

			// create and run the task
			VanillaDb.taskMgr().runTask(spt);
		}
	}
}
