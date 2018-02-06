package org.vanilladb.comm.demo;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.vanilladb.comm.client.ClientAppl;
import org.vanilladb.comm.client.ClientNodeFailListener;
import org.vanilladb.comm.client.ClientP2pMessageListener;
import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;
import org.vanilladb.comm.messages.TotalOrderMessage;
import org.vanilladb.comm.protocols.utils.Profiler;

public class StartClient implements ClientP2pMessageListener, ClientNodeFailListener {

	final int LONG_MESSAGE_LENGTH = 0;
	final int BATCH_SIZE = 1;

	// this is the message sending interval in milliseconds
	static Profiler p;

	int count;

	int target = 0;

	int myId;

	long timeSent;

	ClientAppl zabappl;

	long totalTime = 0, messageCount = 0;

	String longMessage;

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

	public StartClient(int id) {
		zabappl = new ClientAppl(id, this, this);
		this.myId = id;
		zabappl.start();
	}

	public void onNodeFail(int id) {
		// do nothing
	}

	synchronized public void go() {

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		count = 0;
		String message;
		zabappl.startPFD();

		// zabappl.clientSend("start");
		while (true) {

			timeSent = System.nanoTime();
			++count;

			message = myId + " " + count + " " + generateLongMessage();
			// sendTotalOrderMessage(message);
			// sendP2pMessage(message);

			try {
				this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void sendTotalOrderMessage(Object o) {

		Object[] os = new Object[BATCH_SIZE];
		os[0] = o;
		for (int i = 1; i < BATCH_SIZE; ++i) {
			os[i] = generateLongMessage();
		}
		//zabappl.sendTotalOrderRequest(os);
	}

	public static void main(String args[]) throws InterruptedException {
		p = new Profiler();
		p.startCollecting();
		StartClient zab = new StartClient(Integer.parseInt(args[0]));
		zab.go();
	}

	synchronized public void onRecvClientP2pMessage(P2pMessage o) {
		handleMessage(o.getMessage());
	}

	public void handleMessage(Object o) {
		String s = (String) o;
		if ((myId + " " + count).equals(s.substring(0,
				s.indexOf(' ', s.indexOf(' ') + 1)))) {
			long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
					- timeSent);
			System.out.println(count + " time: " + (latency));
			totalTime += latency;
			messageCount++;
			if (count % 1000 == 0)
				System.out.println("average: " + (totalTime / messageCount)
						+ " ms, "
						+ ((double) messageCount / ((double) totalTime / 1000))
						+ " mps");
			this.notifyAll();
		}
		if (count % 1000 == 0) {
			System.out.println(p.getTopMethods(10));
			// System.out.println(p.getTopLines(20));
			p.startCollecting();
		}
	}

	@Override
	public void onNodeFail(int id, ChannelType channelType) {
		// TODO Auto-generated method stub
		
	}

}
