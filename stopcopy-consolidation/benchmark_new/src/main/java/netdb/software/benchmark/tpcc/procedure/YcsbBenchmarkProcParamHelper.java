package netdb.software.benchmark.tpcc.procedure;

import java.util.HashMap;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.server.VanillaDdDb;

import netdb.software.benchmark.tpcc.TpccConstants;

public class YcsbBenchmarkProcParamHelper extends StoredProcedureParamHelper {
	
	public static void main(String[] args) {
		YcsbBenchmarkProcParamHelper helpler = new YcsbBenchmarkProcParamHelper();
		
		helpler.prepareParameters(new Object[]{1, 277270, 0, 0});
		helpler.prepareParameters(new Object[]{1, 166894, 1, 166894,
				"ONp4MASFaSCDvuAQnx9z7utkjoPo5HZzD", 1, 1001520, "RJB5hdtldIVdUP0jzCPlALHV97F2PR2wm"});
	}
	
	private int readCount;
	private int writeCount;
	private int insertCount;
	private String[] readIds;
	private String[] writeIds;
	private String[] writeVals;
	private String[] insertIds;
	private String[] insertVals; // All fields use the same value
	
	private int latestId = -1;

	public int getReadCount() {
		return readCount;
	}

	public int getWriteCount() {
		return writeCount;
	}
	
	public int getInsertCount() {
		return insertCount;
	}
	
	public String getReadId(int index) {
		return readIds[index];
	}
	
	public String getWriteId(int index) {
		return writeIds[index];
	}
	
	public String getWriteValue(int index) {
		return writeVals[index];
	}
	
	public String getInsertId(int index) {
		return insertIds[index];
	}
	
	public int getLatestIdInParam() {
		return latestId;
	}
	
	public HashMap<String, Constant> getInsertVals(int index) {
		HashMap<String, Constant> fldVals = new HashMap<String, Constant>();
		
		fldVals.put("ycsb_id", new VarcharConstant(insertIds[index]));
		for (int count = 1; count < TpccConstants.YCSB_FIELD_COUNT; count++)
			fldVals.put("ycsb_" + count, new VarcharConstant(insertVals[index]));
		
		return fldVals;
	}

	@Override
	public void prepareParameters(Object... pars) {
		int indexCnt = 0;

		readCount = (Integer) pars[indexCnt++];
		readIds = new String[readCount];
		for (int i = 0; i < readCount; i++)
			readIds[i] = String.format(TpccConstants.YCSB_ID_FORMAT, (Integer) pars[indexCnt++]);

		writeCount = (Integer) pars[indexCnt++];
		writeIds = new String[writeCount];
		for (int i = 0; i < writeCount; i++)
			writeIds[i] = String.format(TpccConstants.YCSB_ID_FORMAT, (Integer) pars[indexCnt++]);
		writeVals = new String[writeCount];
		for (int i = 0; i < writeCount; i++)
			writeVals[i] = (String) pars[indexCnt++];
		
		insertCount = (Integer) pars[indexCnt++];
		if (insertCount > 0)
			latestId = (Integer) pars[indexCnt];
		insertIds = new String[insertCount];
		for (int i = 0; i < insertCount; i++)
			insertIds[i] = String.format(TpccConstants.YCSB_ID_FORMAT, (Integer) pars[indexCnt++]);
		insertVals = new String[insertCount];
		for (int i = 0; i < insertCount; i++)
			insertVals[i] = (String) pars[indexCnt++];

		if (writeCount == 0 && insertCount == 0)
			setReadOnly(true);
	}

	@Override
	public SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		
		// Sender
		sch.addField("sender", Type.INTEGER);
		rec.setVal("sender", new IntegerConstant(VanillaDdDb.serverId()));

		return new SpResultSet(sch, rec);
	}

}
