package org.vanilladb.comm.demo;

import java.sql.Time;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.helpers.DateTimeDateFormat;
import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;
import org.vanilladb.comm.messages.TotalOrderMessage;
import org.vanilladb.comm.protocols.utils.Profiler;
import org.vanilladb.comm.server.ServerAppl;
import org.vanilladb.comm.server.ServerNodeFailListener;
import org.vanilladb.comm.server.ServerP2pMessageListener;
import org.vanilladb.comm.server.ServerTotalOrderedMessageListener;

public class StartServer implements ServerTotalOrderedMessageListener,
		ServerP2pMessageListener, ServerNodeFailListener {

	// this is the message sending interval in milliseconds
	static Profiler p;
	int pcount = 0;

	int LONG_MESSAGE_LENGTH = 10000;

	long totalTime = 0, messageCount = 0;
	int target;

	int count;

	boolean flag;

	int myId;

	long timeSent;

	ServerAppl zabappl;

	public StartServer(int id) {
		zabappl = new ServerAppl(id, this, this, this);
		this.myId = id;
		zabappl.start();
	}

	public static void main(String args[]) throws InterruptedException {

		StartServer zabServer = new StartServer(Integer.parseInt(args[0]));
		Thread.sleep(10000);
		p = new Profiler();
		p.startCollecting();
		zabServer.zabappl.startPFD();
		zabServer.go();

	}

	synchronized public void onRecvServerTotalOrderedMessage(TotalOrderMessage o) {
		// handleMessage(o.getMessage());
		pcount++;
		if (pcount % 1000 == 0) {
			System.out.println(p.getTopMethods(10));
			// System.out.println(p.getTopLines(20));
			p.startCollecting();
		}
	}

	public void onNodeFail(int id) {

	}

	synchronized public void onRecvServerP2pMessage(P2pMessage o) {
		final int sender = Integer.parseInt(((String) o.getMessage())
				.split(" ")[0]);
		if (sender != myId) {
			final P2pMessage rm = o;
			new Thread() {
				public void run() {
					rm.getMessage();
					P2pMessage m = new P2pMessage(rm.getMessage(), sender,
							rm.getGroup());
					zabappl.sendP2pMessage(m);
				}
			}.start();
		} else {
			handleMessage((String) o.getMessage());
		}
		if (count % 1000 == 0) {
			System.out.println(p.getTopMethods(10));
			// System.out.println(p.getTopLines(20));
			p.startCollecting();
		}
	}

	public void sendP2pMessage(Object o) {
		do {
			target = (target + 1) % 3;
		} while (target != myId);
		P2pMessage m = new P2pMessage(o, target, ChannelType.SERVER);
		zabappl.sendP2pMessage(m);
	}

	public void sendTotalOrderMessage(Object o) {
		Object[] os = { o };

		zabappl.sendTotalOrderRequest(os);
	}

	synchronized public void go() {

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long longStartTime = 0;
		count = 0;
		String message;
		zabappl.startPFD();
		while (true) {
			if (count % 100000 == 0) {
				System.out.println("Last 10000 round took: "
						+ TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
								- longStartTime));
				longStartTime = System.nanoTime();
			}
			timeSent = System.nanoTime();
			++count;

			message = myId + " " + count + " " + generateLongMessage();
			// sendTotalOrderMessage(message);

			sendP2pMessage(message);

			try {
				this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void handleMessage(String s) {
		if ((myId + " " + count).equals(s.substring(0,
				s.indexOf(' ', s.indexOf(' ') + 1)))) {
			long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
					- timeSent);
			// System.out.println(count + " time: " + (latency));
			totalTime += latency;
			messageCount++;
			if (count % 1000 == 0)
				System.out.println("average: " + (totalTime / messageCount)
						+ " ms, " + (messageCount / totalTime) + " mps");
			this.notifyAll();
		}
	}

	private String generateLongMessage() {
		char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_.01234567890"
				.toCharArray();
		StringBuilder sb = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < LONG_MESSAGE_LENGTH; ++i) {
			char c = chars[random.nextInt(chars.length)];
			sb.append(c);
		}
		return sb.toString();
	}

	@Override
	public void onNodeFail(int id, ChannelType channelType) {
		// TODO Auto-generated method stub
		
	}
}
