package org.vanilladb.core.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TimerStatistics {
	
	private static final long REPORT_PERIOD = 3000;
	private static final AtomicBoolean IS_REPORTING = new AtomicBoolean(false); 
	
	private static List<Object> components = new LinkedList<Object>();
	private static Map<Object, ComponentStatMem> stats = new ConcurrentHashMap<Object, ComponentStatMem>();
	private static Object[] anchors = new Object[1099];
	
	private static class ComponentStatMem {
		long recordTime;
		int count;
		
		synchronized void addTime(long time) {
			recordTime += time;
			count++;
		}
		
		synchronized ComponentStatRecord calculate() {
			double avg = 0;
			if (count != 0) {
				avg = ((double) recordTime) / ((double) count);
			}
			
			ComponentStatRecord rec = new ComponentStatRecord();
			rec.average = avg;
			rec.count = count;
			
			// Reset
			recordTime = 0;
			count = 0;
			
			return rec;
		}
	}
	
	private static class ComponentStatRecord {
		Object component;
		long count;
		double average;
	}
	
	static {
		for (int i = 0; i < anchors.length; i++)
			anchors[i] = new Object();
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				long startTime = System.currentTimeMillis();
				long lastTime = startTime;
				
				while (true) {
					long currentTime = System.currentTimeMillis();
					
					if (currentTime - lastTime > REPORT_PERIOD) {
						lastTime = currentTime;
						
						double t = (currentTime - startTime) / 1000;
						
						// Retrieve the statistics
						List<ComponentStatRecord> records = new ArrayList<ComponentStatRecord>();
						ComponentStatRecord exeRec = null;
						
						synchronized (components) {
							for (Object key : components) {
								ComponentStatMem stat = stats.get(key);
								ComponentStatRecord rec = stat.calculate();
								
								if (rec.count > 100) {
									rec.component = key;
									
									if (key.equals(Timer.EXE_TIME_KEY)) {
										exeRec = rec;
									} else {
										records.add(rec);
									}
								}
							}
						}
						
						// Sort the records by average time (in reverse order)
						Collections.sort(records, new Comparator<ComponentStatRecord>() {
							@Override
							public int compare(ComponentStatRecord o1, ComponentStatRecord o2) {
								if (o1.average < o2.average)
									return 1;
								else if (o1.average > o2.average)
									return -1;
								return 0;
							}
						});
						
						StringBuilder sb = new StringBuilder();
						
						sb.append("===================================\n");
						sb.append(String.format("At %.2f:\n", t));
						
						// Only show the top 10
						int top = 0;
						
						for (ComponentStatRecord rec : records) {
							sb.append(String.format("%s: average %.2f us", rec.component, rec.average));
							sb.append(String.format(" with count %d\n", rec.count));
							
							top++;
							if (top >= 10)
								break;
						}
						
						if (exeRec != null) {
							sb.append(String.format("%s: average %.2f us", exeRec.component, exeRec.average));
							sb.append(String.format(" with count %d\n", exeRec.count));
						}
						
						sb.append("===================================\n");
						
						if (IS_REPORTING.get())
							System.out.println(sb.toString());
					}
					
					try {
						Thread.sleep(REPORT_PERIOD / 10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
		}).start();
	}
	
	public static void startReporting() {
		IS_REPORTING.set(true);
	}
	
	public static void stopReporting() {
		IS_REPORTING.set(false);
	}
	
	static void add(Timer timer) {
		if (IS_REPORTING.get()) {
			for (Object com : timer.getComponents()) {
				ComponentStatMem stat = stats.get(com);
				if (stat == null)
					stat = createNewStatistic(com);
				stat.addTime(timer.getComponentTime(com));
			}
		}
	}
	
	private static Object getAnchor(Object key) {
		int code = key.hashCode();
		code = Math.abs(code); // avoid negative value
		return anchors[code % anchors.length];
	}
	
	private static ComponentStatMem createNewStatistic(Object key) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			
			ComponentStatMem stat = stats.get(key);
			
			if (stat == null) {
				stat = new ComponentStatMem();
				stats.put(key, new ComponentStatMem());
				synchronized (components) {
					components.add(key);
				}
			}
			
			return stat;
		}
	}
}
