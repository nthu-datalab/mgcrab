package org.vanilladb.core.util;

import java.util.LinkedList;

public class DelayPrinter implements Runnable {

	private long delay;
	private static DelayPrinter self;
	private LinkedList<String> messages = new LinkedList<String>();
	private LinkedList<String> stagedMessages = new LinkedList<String>();

	static {
		self = new DelayPrinter(10000);
		new Thread(self).start();
	}

	public static DelayPrinter shareInstance() {
		return self;
	}

	private DelayPrinter(long delay) {
		this.delay = delay;
	}

	public void print(String message) {
		synchronized (messages) {
			messages.add(message);
		}
	}

	public void removeMessage(String message) {
		synchronized (messages) {
			messages.remove(message);
		}
	}

	@Override
	public void run() {
		while (true) {
			synchronized (messages) {
				stagedMessages = new LinkedList<String>(messages);
				messages = new LinkedList<String>();
			}

			if (stagedMessages.size() > 0) {
				for (String s : stagedMessages) {
					if (s != null)
						System.out.println(s);
				}
			}

			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
