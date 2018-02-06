package org.vanilladb.dd.remote.groupcomm.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;
import org.vanilladb.comm.messages.TotalOrderMessage;
import org.vanilladb.comm.server.ServerAppl;
import org.vanilladb.comm.server.ServerNodeFailListener;
import org.vanilladb.comm.server.ServerP2pMessageListener;
import org.vanilladb.comm.server.ServerTotalOrderedMessageListener;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.cache.tpart.NewTPartCacheMgr;
import org.vanilladb.dd.remote.groupcomm.ClientResponse;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.remote.groupcomm.Tuple;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.VanillaDdDb.ServiceType;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class ConnectionMgr
		implements ServerTotalOrderedMessageListener, ServerP2pMessageListener, ServerNodeFailListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());

	public static final int SEQ_NODE_ID = PartitionMetaMgr.NUM_PARTITIONS;

	private ServerAppl serverAppl;
	private int myId;
	private BlockingQueue<TotalOrderMessage> tomQueue = new LinkedBlockingQueue<TotalOrderMessage>();
	private BlockingQueue<TupleSet> tupleSetQueue = new LinkedBlockingQueue<TupleSet>();

	private final static long FAKE_NETWORK_LATENCY;

	static {
		String prop = System.getProperty(ConnectionMgr.class.getName() + ".FAKE_NETWORK_LATENCY");
		FAKE_NETWORK_LATENCY = (prop == null) ? 0 : Long.parseLong(prop.trim());
	}

	public ConnectionMgr(int id) {
		myId = id;
		serverAppl = new ServerAppl(id, this, this, this);
		serverAppl.start();

		// wait for all servers to start up
		if (logger.isLoggable(Level.INFO))
			logger.info("wait for all servers to start up comm. module");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		serverAppl.startPFD();
		VanillaDb.taskMgr().runTask(new Task() {

			@Override
			public void run() {
				while (true) {
					try {
						TotalOrderMessage tom = tomQueue.take();
						for (int i = 0; i < tom.getMessages().length; ++i) {
							StoredProcedureCall spc = (StoredProcedureCall) tom.getMessages()[i];
							spc.setTxNum(tom.getTotalOrderIdStart() + i);
							VanillaDdDb.scheduler().schedule(spc);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		});
		
		VanillaDb.taskMgr().runTask(new Task() {

			@Override
			public void run() {
				while (true) {
					try {
						TupleSet ts = tupleSetQueue.take();
						// For special sink ids
						switch (ts.sinkId()) {
						case MigrationManager.SINK_ID_START_MIGRATION:
							VanillaDdDb.migrationMgr().onReceiveStartMigrationReq(ts.getMetadata());
							break;
						case MigrationManager.SINK_ID_ANALYSIS:
							VanillaDdDb.migrationMgr().onReceiveAnalysisReq(ts.getMetadata());
							break;
						case MigrationManager.SINK_ID_ASYNC_PUSHING:
							VanillaDdDb.migrationMgr().onReceiveAsyncMigrateReq(ts.getMetadata());
							break;
						case MigrationManager.SINK_ID_START_CRABBING:
							VanillaDdDb.migrationMgr().onReceiveStartCrabbingReq(ts.getMetadata());
							break;
						case MigrationManager.SINK_ID_START_CATCH_UP:
							VanillaDdDb.migrationMgr().onReceiveStartCatchUpReq(ts.getMetadata());
							break;
						case MigrationManager.SINK_ID_STOP_MIGRATION:
							VanillaDdDb.migrationMgr().onReceiveStopMigrateReq(ts.getMetadata());
							break;
						}
						// For database servers
						for (Tuple t : ts.getTupleSet()) {
//							if (!VanillaDdDb.migrationMgr().keyIsInMigrationRange(t.key))
//								System.out.println("Receive a tuple: " + t);
							if (VanillaDdDb.serviceType() == ServiceType.CALVIN) {
								CalvinCacheMgr cacheMgr = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
								cacheMgr.cacheRemoteRecord(t.key, t.rec);
							} else if (VanillaDdDb.serviceType() == ServiceType.TPART) {
								if (t.rec == null)
									throw new RuntimeException("recv. null record tx:" + t.srcTxNum + " key:" + t.key);
								((NewTPartCacheMgr) VanillaDdDb.cacheMgr()).cacheRemoteRecord(t.key, t.srcTxNum,
										t.destTxNum, t.rec);
							} else
								throw new IllegalArgumentException("Service Type Not Found Exception");
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		// call the communication module to send the response back to client
		P2pMessage p2pmsg = new P2pMessage(new ClientResponse(clientId, rteId, txNum, rs), clientId,
				ChannelType.CLIENT);
		serverAppl.sendP2pMessage(p2pmsg);
	}

	public void callStoredProc(int pid, Object... pars) {
		StoredProcedureCall[] spcs = { new StoredProcedureCall(myId, pid, pars) };
		serverAppl.sendTotalOrderRequest(spcs);
	}

	public void sendBroadcastRequest(Object[] objs) {
		serverAppl.sendBroadcastRequest(objs);
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
		if (FAKE_NETWORK_LATENCY > 0) {
			try {
				Thread.sleep(FAKE_NETWORK_LATENCY);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		P2pMessage p2pmsg = new P2pMessage(reading, nodeId, ChannelType.SERVER);
		serverAppl.sendP2pMessage(p2pmsg);
	}

	@Override
	public void onRecvServerP2pMessage(final P2pMessage p2pmsg) {
		Object msg = p2pmsg.getMessage();
		if (msg.getClass().equals(TupleSet.class))
			try {
				tupleSetQueue.put((TupleSet) msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		else
			throw new IllegalArgumentException();
		
//		new Thread() {
//			@Override
//			public void run() {
//				Object msg = p2pmsg.getMessage();
//				if (msg.getClass().equals(TupleSet.class)) {
//					TupleSet ts = (TupleSet) msg;
//
//					// For special sink ids
//					switch (ts.sinkId()) {
//					case MigrationManager.SINK_ID_ANALYSIS:
//						VanillaDdDb.migrationMgr().onReceiveAnalysisReq(ts.getMetadata());
//						break;
//					case MigrationManager.SINK_ID_ASYNC_PUSHING:
//						VanillaDdDb.migrationMgr().onReceiveAsyncMigrateReq(ts.getMetadata());
//						break;
//					case MigrationManager.SINK_ID_START_CATCH_UP:
//						VanillaDdDb.migrationMgr().onReceiveStartCatchUpReq(ts.getMetadata());
//						break;
//					case MigrationManager.SINK_ID_STOP_MIGRATION:
//						VanillaDdDb.migrationMgr().onReceiveStopMigrateReq(ts.getMetadata());
//						break;
//					}
//					// For database servers
//					for (Tuple t : ts.getTupleSet()) {
//						if (!VanillaDdDb.migrationMgr().keyIsInMigrationRange(t.key))
//							System.out.println("Receive a tuple: " + t);
//						if (VanillaDdDb.serviceType() == ServiceType.CALVIN) {
//							CalvinCacheMgr cacheMgr = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
//							cacheMgr.cacheRemoteRecord(t.key, t.rec);
//						} else if (VanillaDdDb.serviceType() == ServiceType.TPART) {
//							if (t.rec == null)
//								throw new RuntimeException("recv. null record tx:" + t.srcTxNum + " key:" + t.key);
//							((NewTPartCacheMgr) VanillaDdDb.cacheMgr()).cacheRemoteRecord(t.key, t.srcTxNum,
//									t.destTxNum, t.rec);
//						} else
//							throw new IllegalArgumentException("Service Type Not Found Exception");
//					}
//				} else
//					throw new IllegalArgumentException();
//			}
//		}.start();
	}

	@Override
	public void onRecvServerTotalOrderedMessage(TotalOrderMessage tom) {
		// for (int i = 0; i < tom.getMessages().length; ++i) {
		// StoredProcedureCall spc = (StoredProcedureCall) tom.getMessages()[i];
		// spc.setTxNum(tom.getTotalOrderIdStart() + i);
		// VanillaDdDb.scheduler().schedule(spc);
		// }
		try {
			tomQueue.put(tom);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void onNodeFail(int id, ChannelType ct) {
		// do nothing
	}
}
