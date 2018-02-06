package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.sql.RecordKey;

import netdb.software.benchmark.tpcc.procedure.MicroBenchmarkProcParamHelper;

public class MicroBenchmarkProc extends
		CalvinStoredProcedure<MicroBenchmarkProcParamHelper> {

	private Map<RecordKey, Constant> writeConstantMap = new HashMap<RecordKey, Constant>();

	public MicroBenchmarkProc(long txNum) {
		super(txNum, new MicroBenchmarkProcParamHelper());
	}

	@Override
	public void prepareKeys() {
		// set read keys
		for (int i : paramHelper.getReadItemId()) {
			// create record key for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(i));
			RecordKey key = new RecordKey("item", keyEntryMap);
			addReadKey(key);
		}

		// set write keys
		int[] writeItemIds = paramHelper.getWriteItemId();
		double[] newItemPrices = paramHelper.getNewItemPrice();
		for (int i = 0; i < writeItemIds.length; i++) {
			// create record key for writing
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(writeItemIds[i]));
			RecordKey key = new RecordKey("item", keyEntryMap);
			addWriteKey(key);

			// Create key-value pairs for writing
			Constant c = new DoubleConstant(newItemPrices[i]);
			writeConstantMap.put(key, c);
		}
	}

	@Override
	protected void onLocalReadCollected(
			Map<RecordKey, CachedRecord> localReadings) {
		// Do nothing
	}

	@Override
	protected void onRemoteReadCollected(
			Map<RecordKey, CachedRecord> remoteReadings) {
		// Do nothing
	}

	@Override
	protected void writeRecords(Map<RecordKey, CachedRecord> readings) {
		Set<RecordKey> localWriteKeys = getLocalWriteKeys();

		// UPDATE items SET i_price = ... WHERE i_id = ...
		for (RecordKey key : localWriteKeys) {
			Constant newPrice = writeConstantMap.get(key);
			CachedRecord rec = readings.get(key);

			rec.setVal("i_price", newPrice);
			update(key, rec);
		}
	}

	@Override
	protected void masterCollectResults(Map<RecordKey, CachedRecord> readings) {
		// SELECT i_name, i_price FROM items WHERE i_id = ...
		for (CachedRecord rec : readings.values()) {
			rec.getVal("i_name").asJavaVal();
			rec.getVal("i_price").asJavaVal();
		}
	}
}
