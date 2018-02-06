package org.vanilladb.dd.schedule.oda;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.cache.oda.OdaCacheMgr;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.oda.OdaStoredProcedureTask;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public class OdaScheduler extends Task implements Scheduler {
	private static final String FACTORY_CLASS;

	private OdaStoredProcedureFactory factory;

	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();

	private OdaCacheMgr cm;

	public static int CACHE_TTL = 10;

	static {
		String prop = System.getProperty(OdaScheduler.class.getName()
				+ ".FACTORY_CLASS");

		if (prop != null && !prop.isEmpty())
			FACTORY_CLASS = prop.trim();
		else
			throw new RuntimeException("Factory property is empty");

		prop = System.getProperty(OdaScheduler.class.getName() + ".CACHE_TTL");
		CACHE_TTL = (prop == null) ? 1000 : Integer.parseInt(prop);
	}

	public OdaScheduler() {
		Class<?> c;
		try {
			c = Class.forName(FACTORY_CLASS);
			if (c != null)
				factory = (OdaStoredProcedureFactory) c.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(
					"Factory doesn't exist, or can not be access");
		}

		cm = (OdaCacheMgr) VanillaDdDb.cacheMgr();
	}

	public void schedule(StoredProcedureCall... calls) {
		try {
			for (int i = 0; i < calls.length; i++) {
				spcQueue.put(calls[i]);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				StoredProcedureCall call = spcQueue.take();
				// log request

				if (call.isNoOpStoredProcCall())
					continue;

				// create procedure task
				OdaStoredProcedure sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				OdaStoredProcedureTask spt = new OdaStoredProcedureTask(
						call.getClientId(), call.getRteId(), call.getTxNum(),
						sp);
				sp.prepare(call.getPars());
				sp.prepareKeys();

				if (!sp.isReadOnly())
					DdRecoveryMgr.logRequest(call);

				// set read links for the created node
				Map<RecordKey, Long> readLinks = new HashMap<RecordKey, Long>();
				if (spt.getReadSet() != null)
					for (RecordKey res : spt.getReadSet()) {

						Long latestVersion = cm
								.prepareLatestVersionForTransaction(res,
										spt.getTxNum());
						readLinks.put(res, latestVersion);
					}

				// update the latest version table according to the write set
				if (spt.getWriteSet() != null)
					for (RecordKey res : spt.getWriteSet()) {
						cm.setLatestVersionOfRecord(res, spt.getTxNum());
					}

				// set the read links to task
				spt.setReadLinks(readLinks);

				// create and run the task
				VanillaDdDb.taskMgr().runTask(spt);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}
