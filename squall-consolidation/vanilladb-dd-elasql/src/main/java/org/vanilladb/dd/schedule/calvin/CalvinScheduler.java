package org.vanilladb.dd.schedule.calvin;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.server.task.calvin.CalvinStoredProcedureTask;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public class CalvinScheduler extends Task implements Scheduler {
	private static final String FACTORY_CLASS;

	public static volatile long time, timedTx = -1;

	private CalvinStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();

	static {
		String prop = System.getProperty(CalvinScheduler.class.getName()
				+ ".FACTORY_CLASS");

		if (prop != null && !prop.isEmpty())
			FACTORY_CLASS = prop.trim();
		else
			throw new RuntimeException("Factory property is empty");
	}

	public CalvinScheduler() {
		Class<?> c;
		try {
			c = Class.forName(FACTORY_CLASS);
			if (c != null)
				factory = (CalvinStoredProcedureFactory) c.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(
					"Factory doesn't exist, or can not be access");
		}
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
//		long sum = 0;
//		long count = 0;
//		long recordStart = System.currentTimeMillis();
		
		while (true) {
			try {
				// retrieve stored procedure call
				StoredProcedureCall call = spcQueue.take();
				if (call.isNoOpStoredProcCall())
					continue;
				
//				long startTime = System.nanoTime();

				// create store procedure and prepare
				CalvinStoredProcedure<?> sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());

				// log request
				if (!sp.isReadOnly())
					DdRecoveryMgr.logRequest(call);

				// if this node doesn't have to participate this transaction,
				// skip it
				if (!sp.isParticipated()) {
					continue;
				}

				// create a new task for multi-thread
				CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
						call.getClientId(), call.getRteId(), call.getTxNum(),
						sp);

				// perform conservative locking
				spt.lockConservatively();

				// hand over to a thread to run the task
				VanillaDb.taskMgr().runTask(spt);
				
//				long time = System.nanoTime() - startTime;
//				sum += (time / 1000);
//				count++;
				
//				if (System.currentTimeMillis() - recordStart > 1000) {
//					System.out.println("Avg. Scheduling time: " + (sum / count));
//					sum = 0;
//					count = 0;
//					recordStart = System.currentTimeMillis();
//				}
				

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
