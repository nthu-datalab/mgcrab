package netdb.software.benchmark.tpcc.procedure.vanilladddb.oda;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import netdb.software.benchmark.tpcc.procedure.MicroBenchmarkProcParamHelper;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.schedule.oda.OdaStoredProcedure;
import org.vanilladb.dd.sql.RecordKey;

public class MicroBenchmarkProc extends OdaStoredProcedure {

	private MicroBenchmarkProcParamHelper mbParamHelper = new MicroBenchmarkProcParamHelper();
	private RecordKey[] readKeys, writeKeys;

	public MicroBenchmarkProc(long txNum) {
		this.txNum = txNum;
		this.paramHelper = mbParamHelper;
	}

	@Override
	public void prepareKeys() {

		Map<String, Constant> keyEntryMap;
		RecordKey key;
		List<RecordKey> readKeyList = new ArrayList<RecordKey>();
		List<RecordKey> writeKeyList = new ArrayList<RecordKey>();

		// readings
		for (int i : mbParamHelper.getReadItemId()) {
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(i));
			key = new RecordKey("item", keyEntryMap);
			readKeyList.add(key);
			addReadKey(key);
		}
		// writings
		for (int i = 0; i < mbParamHelper.getWriteCount(); i++) {
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id",
					new IntegerConstant(mbParamHelper.getWriteItemId()[i]));
			key = new RecordKey("item", keyEntryMap);
			writeKeyList.add(key);
			addWriteKey(key);
		}

		readKeys = readKeyList.toArray(new RecordKey[0]);
		writeKeys = writeKeyList.toArray(new RecordKey[0]);
	}

	public void executeSql() {

		for (RecordKey k : readKeys) {
			String name;
			double price;
			CachedRecord rec = read(k);
			// System.out.println("Txn " + txNum + " want read record key " + k
			// + " and get " + rec);
			name = (String) rec.getVal("i_name").asJavaVal();
			price = (Double) rec.getVal("i_price").asJavaVal();

		}

		// write local items
		Map<String, Constant> fldVals;
		// update the items

		for (int i = 0; i < mbParamHelper.getWriteCount(); i++) {
			CachedRecord rec = read(writeKeys[i]);
			write(writeKeys[i], "i_price",
					new DoubleConstant(mbParamHelper.getNewItemPrice()[i]));
		}

	}

}
