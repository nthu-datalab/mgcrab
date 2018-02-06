package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

import netdb.software.benchmark.tpcc.TestingParameters;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.procedure.TestbedLoaderProcParamHelper;
import netdb.software.benchmark.tpcc.util.DoublePlainPrinter;
import netdb.software.benchmark.tpcc.util.RandomPermutationGenerator;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;
import netdb.software.benchmark.tpcc.vanilladddb.metadata.TpccPartitionMetaMgr;

public class TestbedLoaderProc extends
		AllExecuteProcedure<TestbedLoaderProcParamHelper> {
	private static Logger logger = Logger.getLogger(TestbedLoaderProc.class
			.getName());

	private RandomValueGenerator rg = new RandomValueGenerator();

	public TestbedLoaderProc(long txNum) {
		super(txNum, new TestbedLoaderProcParamHelper());
		forceReadWriteTx = true;
	}

	// XXX: We should lock those tables
	// @Override
	// protected void prepareKeys() {
	// List<String> writeTables = Arrays.asList(paramHelper.getTables());
	// localWriteTables.addAll(writeTables);
	// }

	@Override
	protected void executeSql() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		// TODO: remove this hack code in the future
		RecoveryMgr.logSetVal(false);

		// Generate item records
		// XXX: This may not be a good design
		if (TestingParameters.IS_MICROBENCHMARK) {
			// XXX: Disable for YCSB
//			int itemPerPartition = TpccConstants.NUM_ITEMS
//					/ PartitionMetaMgr.NUM_PARTITIONS;
//			int startIId = VanillaDdDb.serverId() * itemPerPartition + 1;
//			int endIId = (VanillaDdDb.serverId() + 1) * itemPerPartition;
//			generateItems(startIId, endIId);
			generateYcsbTableForThisPartition();
		} else
			// Item table need to be fully replicated
			generateItems(1, TpccConstants.NUM_ITEMS);

		// Generate warehouse
		// XXX: This may not be a good design
		if (!TestingParameters.IS_MICROBENCHMARK) {
//			int wPerPart = TpccConstants.NUM_WAREHOUSES
//					/ PartitionMetaMgr.NUM_PARTITIONS;
			int wPerPart = TpccPartitionMetaMgr.WAREHOUSE_PER_PART;
			int startWid = VanillaDdDb.serverId() * wPerPart + 1;
			int endWid = (VanillaDdDb.serverId() + 1) * wPerPart;
			
			if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getSourcePartition()) {
				startWid = TpccConstants.NUM_WAREHOUSES;
				endWid = TpccConstants.NUM_WAREHOUSES;
			} else if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition()) {
				startWid = wPerPart * VanillaDdDb.migrationMgr().getSourcePartition() + 1;
				endWid = TpccConstants.NUM_WAREHOUSES - 1;
			}
			
			if (startWid <= TpccConstants.NUM_WAREHOUSES)
				for (int wid = startWid; wid <= endWid; wid++)
					generateWarehouseInstance(wid);
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		// TODO: remove this hack code in the future
		RecoveryMgr.logSetVal(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		// Delete the log file and create a new one
		VanillaDb.logMgr().removeAndCreateNewLog();

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished.");
	}

	private void generateItems(int startIId, int endIId) {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating items from i_id=" + startIId
					+ " to i_id=" + endIId);

		int iid, iimid;
		String iname, idata;
		double iprice;
		String sql;
		for (int i = startIId; i <= endIId; i++) {
			iid = i;

			// Randomly generate values
			/*
			 * iimid = rg.number(TpccConstants.MIN_IM, TpccConstants.MAX_IM);
			 * iname = rg.randomAString(TpccConstants.MIN_I_NAME,
			 * TpccConstants.MAX_I_NAME); iprice =
			 * rg.fixedDecimalNumber(TpccConstants.MONEY_DECIMALS,
			 * TpccConstants.MIN_PRICE, TpccConstants.MAX_PRICE); idata =
			 * rg.randomAString(TpccConstants.MIN_I_DATA,
			 * TpccConstants.MAX_I_DATA); if (Math.random() < 0.1) idata =
			 * fillOriginal(idata);
			 */

			// Deterministic value generation by item id
			iimid = iid % (TpccConstants.MAX_IM - TpccConstants.MIN_IM)
					+ TpccConstants.MIN_IM;
			iname = String.format("%0" + TpccConstants.MIN_I_NAME + "d", iid);
			iprice = (iid % (int) (TpccConstants.MAX_PRICE - TpccConstants.MIN_PRICE))
					+ TpccConstants.MIN_PRICE;
			idata = String.format("%0" + TpccConstants.MIN_I_DATA + "d", iid);

			sql = "INSERT INTO item(i_id, i_im_id, i_name, i_price, i_data) VALUES ("
					+ iid
					+ ", "
					+ iimid
					+ ", '"
					+ iname
					+ "', "
					+ DoublePlainPrinter.toPlainString(iprice)
					+ ", '"
					+ idata
					+ "' )";

			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}

		if (logger.isLoggable(Level.FINE))
			logger.info("Populating items completed.");
	}

	private void generateWarehouseInstance(int wid) {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating warehouse " + wid);

		generateWarehouse(wid);
		generateStocks(wid);
		generateDistricts(wid);

		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating customers for warehouse " + wid);

		int numDist = TpccConstants.DISTRICTS_PER_WAREHOUSE;
		for (int i = 1; i <= numDist; i++)
			generateCustomers(wid, i);
		for (int i = 1; i <= numDist; i++)
			generateCustomerHistory(wid, i);
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating order for warehouse " + wid);
		for (int i = 1; i <= numDist; i++)
			generateOrders(wid, i);
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating new order for warehouse " + wid);
		for (int i = 1; i <= numDist; i++)
			generateNewOrders(wid, i);
		if (logger.isLoggable(Level.FINE))
			logger.info("Finish populating warehouse " + wid);
	}

	private void generateWarehouse(int wid) {
		double wtax, wytd;
		String wname, wst1, wst2, wcity, wstate, wzip;
		wname = rg.randomAString(6, 10);
		wst1 = rg.randomAString(10, 20);
		wst2 = rg.randomAString(10, 20);
		wcity = rg.randomAString(10, 20);
		wstate = rg.randomAString(2);
		wzip = makeZip();
		wtax = makeTax();
		wytd = TpccConstants.INITIAL_W_YTD;
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO warehouse(w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) ");
		sb.append("values (").append(wid).append(", '").append(wname);
		sb.append("', '").append(wst1).append("', '").append(wst2);
		sb.append("', '").append(wcity).append("', '").append(wstate);
		sb.append("', '").append(wzip).append("', ")
				.append(DoublePlainPrinter.toPlainString(wtax));
		sb.append(", ").append(DoublePlainPrinter.toPlainString(wytd))
				.append(" )");
		int result = VanillaDb.planner().executeUpdate(sb.toString(), tx);
		if (result <= 0)
			throw new RuntimeException();
	}

	private void generateStocks(int wid) {
		int swid = wid, siid, squantity;
		String sd1, sd2, sd3, sd4, sd5, sd6, sd7, sd8, sd9, sd10, sdata;

		for (int i = 1; i <= TpccConstants.NUM_ITEMS; i++) {
			siid = i;
			squantity = rg.number(TpccConstants.MIN_QUANTITY,
					TpccConstants.MAX_QUANTITY);
			sd1 = rg.randomAString(24);
			sd2 = rg.randomAString(24);
			sd3 = rg.randomAString(24);
			sd4 = rg.randomAString(24);
			sd5 = rg.randomAString(24);
			sd6 = rg.randomAString(24);
			sd7 = rg.randomAString(24);
			sd8 = rg.randomAString(24);
			sd9 = rg.randomAString(24);
			sd10 = rg.randomAString(24);

			// S_DATA
			sdata = rg.randomAString(TpccConstants.MIN_I_DATA,
					TpccConstants.MAX_I_DATA);
			if (Math.random() < 0.1)
				sdata = fillOriginal(sdata);

			String sql = "INSERT INTO stock(s_i_id, s_w_id, s_quantity, "
					+ "s_dist_01, s_dist_02 ,s_dist_03, s_dist_04, s_dist_05,"
					+ "s_dist_06, s_dist_07 ,s_dist_08, s_dist_09, s_dist_10,"
					+ "s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES ("
					+ siid
					+ ", "
					+ swid
					+ ", "
					+ squantity
					+ ", '"
					+ sd1
					+ "', '"
					+ sd2
					+ "', '"
					+ sd3
					+ "', '"
					+ sd4
					+ "', '"
					+ sd5
					+ "', '"
					+ sd6
					+ "', '"
					+ sd7
					+ "', '"
					+ sd8
					+ "', '"
					+ sd9
					+ "', '" + sd10 + "', 0, 0, 0, '" + sdata + "')";
			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}

	private void generateDistricts(int wid) {
		int did;
		double dtax, dytd;
		dytd = TpccConstants.INITIAL_D_YTD;

		String dname, dst1, dst2, dcity, dstate, dzip;
		for (int i = 1; i <= TpccConstants.DISTRICTS_PER_WAREHOUSE; i++) {
			did = i;
			dname = rg.randomAString(6, 10);
			dst1 = rg.randomAString(10, 20);
			dst2 = rg.randomAString(10, 20);
			dcity = rg.randomAString(10, 20);
			dstate = rg.randomAString(2);
			dzip = makeZip();
			dtax = makeTax();

			String sql = "INSERT INTO district(d_id, d_w_id, "
					+ "d_name, d_street_1, d_street_2, d_city, d_state, d_zip,"
					+ " d_tax, d_ytd, d_next_o_id ) VALUES ("
					+ did
					+ ", "
					+ wid
					+ ", '"
					+ dname
					+ "', '"
					+ dst1
					+ "', '"
					+ dst2
					+ "', '"
					+ dcity
					+ "', '"
					+ dstate
					+ "', '"
					+ dzip
					+ "', "
					+ DoublePlainPrinter.toPlainString(dtax)
					+ ", "
					+ DoublePlainPrinter.toPlainString(dytd)
					+ ", "
					+ (TpccConstants.CUSTOMERS_PER_DISTRICT + 1) + ")";
			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}

	private void generateCustomers(int wid, int did) {
		int cid;
		String clast, cmiddle = TpccConstants.MIDDLE, cfirst, cst1, cst2, ccity, cstate, czip, cphone, ccredit, cdata;
		long csince;
		double cdiscount, ccl, cbal, cytdpay;
		ccl = TpccConstants.INITIAL_CREDIT_LIM;
		cbal = TpccConstants.INITIAL_BALANCE;
		cytdpay = TpccConstants.INITIAL_YTD_PAYMENT;

		for (int i = 1; i <= TpccConstants.CUSTOMERS_PER_DISTRICT; i++) {
			cid = i;
			if (i > TpccConstants.NUM_DISTINCT_CLAST) // TpccConstants.CUSTOMERS_PER_DISTRICT/3
				clast = rg.makeRandomLastName(true);
			else
				clast = rg.makeLastName(cid - 1);

			cfirst = rg.randomAString(TpccConstants.MIN_FIRST,
					TpccConstants.MAX_FIRST);
			cst1 = rg.randomAString(10, 20);
			cst2 = rg.randomAString(10, 20);
			ccity = rg.randomAString(10, 20);
			cstate = rg.randomAString(2);
			czip = makeZip();
			cphone = rg.nstring(TpccConstants.PHONE, TpccConstants.PHONE);
			if (Math.random() < 0.1)
				ccredit = TpccConstants.BAD_CREDIT;
			else
				ccredit = TpccConstants.GOOD_CREDIT;
			csince = System.currentTimeMillis();
			cdiscount = rg.fixedDecimalNumber(TpccConstants.DISCOUNT_DECIMALS,
					TpccConstants.MIN_DISCOUNT, TpccConstants.MAX_DISCOUNT);

			cdata = rg.randomAString(TpccConstants.MIN_C_DATA,
					TpccConstants.MAX_C_DATA);

			String sql = "INSERT INTO customer(c_id, c_d_id, c_w_id, "
					+ "c_last, c_middle, c_first, c_street_1, c_street_2, "
					+ "c_city, c_state, c_zip, c_phone, c_since, c_credit,"
					+ "c_credit_lim, c_discount, c_balance, c_ytd_payment, "
					+ "c_payment_cnt, c_delivery_cnt, c_data ) VALUES ("
					+ cid
					+ ","
					+ did
					+ ","
					+ wid
					+ ",'"
					+ clast
					+ "', '"
					+ cmiddle
					+ "', '"
					+ cfirst
					+ "', '"
					+ cst1
					+ "', '"
					+ cst2
					+ "', '"
					+ ccity
					+ "', '"
					+ cstate
					+ "', '"
					+ czip
					+ "', '"
					+ cphone
					+ "', "
					+ csince
					+ ", '"
					+ ccredit
					+ "', "
					+ DoublePlainPrinter.toPlainString(ccl)
					+ ", "
					+ DoublePlainPrinter.toPlainString(cdiscount)
					+ ", "
					+ DoublePlainPrinter.toPlainString(cbal)
					+ ", "
					+ DoublePlainPrinter.toPlainString(cytdpay)
					+ ", 1, 0, '"
					+ cdata + "')";

			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
		if (logger.isLoggable(Level.FINE))
			logger.info("Finish populating customers for district " + did);
	}

	private void generateCustomerHistory(int wid, int did) {
		int hcid;
		Long hdate;
		double hamount = TpccConstants.INITIAL_AMOUNT;
		String hdata;
		for (int i = 1; i <= TpccConstants.CUSTOMERS_PER_DISTRICT; i++) {
			hcid = i;
			hdata = rg.randomAString(TpccConstants.MIN_DATA,
					TpccConstants.MAX_DATA);
			hdate = System.currentTimeMillis();

			String sql = "INSERT INTO history(h_id, h_c_id, h_c_d_id, h_c_w_id, "
					+ "h_d_id,h_w_id, h_date, h_amount, h_data ) VALUES (1, "
					+ hcid + ", " + did + "," + wid + "," + did + "," + wid
					+ "," + hdate + ","
					+ DoublePlainPrinter.toPlainString(hamount) + ", '" + hdata
					+ "')";
			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}

	private void generateOrders(int wid, int did) {
		int oid, ocid, ocarid, ol_cnt;
		long oenrtyd;
		RandomPermutationGenerator rpg = new RandomPermutationGenerator(
				TpccConstants.CUSTOMERS_PER_DISTRICT);
		rpg.next();
		for (int i = 1; i <= TpccConstants.CUSTOMERS_PER_DISTRICT; i++) {
			oid = i;
			ocid = rpg.get(i - 1);
			oenrtyd = System.currentTimeMillis();
			if (i < TpccConstants.NEW_ORDER_START_ID)
				ocarid = rg.number(TpccConstants.MIN_CARRIER_ID,
						TpccConstants.MAX_CARRIER_ID);
			else
				ocarid = TpccConstants.NULL_CARRIER_ID;
			ol_cnt = rg.number(TpccConstants.MIN_OL_CNT,
					TpccConstants.MAX_OL_CNT);

			String sql = "INSERT INTO ORDERS(o_id, o_c_id, o_d_id, "
					+ "o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ("
					+ oid + ", " + ocid + ", " + did + "," + wid + ","
					+ oenrtyd + "," + ocarid + ", " + ol_cnt + ",1)";

			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();

			generateOrderLine(wid, did, i, ol_cnt, oenrtyd);
		}
	}

	private void generateOrderLine(int warehouseId, int districtId, int orderId,
			int ol_cnt, long date) {
		int olnum, oliid;
		long oldeld;
		double olamount;
		String oldistinfo;
		for (int i = 1; i <= ol_cnt; i++) {
			olnum = i;
			oliid = rg.number(1, TpccConstants.NUM_ITEMS);

			if (orderId < TpccConstants.NEW_ORDER_START_ID) {
				oldeld = date;
				olamount = 0.0;
			} else {
				oldeld = TpccConstants.NULL_DELIVERY_DATE;
				olamount = rg.fixedDecimalNumber(TpccConstants.MONEY_DECIMALS,
						TpccConstants.MIN_AMOUNT, TpccConstants.MAX_PRICE
								* TpccConstants.MAX_OL_QUANTITY);
			}

			oldistinfo = rg.randomAString(24);
			String sql = "INSERT INTO order_line(ol_o_id, ol_d_id, "
					+ "ol_w_id, ol_number, ol_i_id, ol_supply_w_id, "
					+ "ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)"
					+ " VALUES (" + orderId + "," + districtId + ","
					+ warehouseId + "," + olnum + "," + oliid + ", "
					+ warehouseId + ", " + oldeld + ", 5, "
					+ DoublePlainPrinter.toPlainString(olamount) + ", '"
					+ oldistinfo + "')";

			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}

	private void generateNewOrders(int wid, int did) {
		int nooid;
		for (int i = TpccConstants.NEW_ORDER_START_ID; i <= TpccConstants.CUSTOMERS_PER_DISTRICT; i++) {
			nooid = i;
			String sql = "INSERT INTO new_order(no_o_id, no_d_id, no_w_id) VALUES ("
					+ nooid + "," + did + "," + wid + ")";
			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}

	private String fillOriginal(String data) {
		int originalLength = TpccConstants.ORIGINAL_STRING.length();
		int position = rg.number(0, data.length() - originalLength);
		String out = data.substring(0, position)
				+ TpccConstants.ORIGINAL_STRING
				+ data.substring(position + originalLength);
		return out;
	}

	private String makeZip() {
		return rg.nstring(4) + TpccConstants.ZIP_SUFFIX;
	}

	private double makeTax() {
		return rg.fixedDecimalNumber(TpccConstants.TAX_DECIMALS,
				TpccConstants.MIN_TAX, TpccConstants.MAX_TAX);
	}
	
	private void generateYcsbTableForThisPartition() {
		// FIXME: Delete for consolidation
		// This code considers the case of migration 
//		if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition()) {
//			// Do nothing
//		} else if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getSourcePartition()) {
//			// Load its data
//			int startId = VanillaDdDb.serverId() * TpccConstants.YCSB_MAX_RECORD_PER_PART + 1;
//			generateYcsbTable(startId, TpccConstants.YCSB_RECORD_PER_PART / 2);
//			// Load the data of the destination node
//			startId = VanillaDdDb.migrationMgr().getDestPartition() * TpccConstants.YCSB_MAX_RECORD_PER_PART + 1;
//			generateYcsbTable(startId, TpccConstants.YCSB_RECORD_PER_PART / 2);
//		} else {
			int startId = VanillaDdDb.serverId() * TpccConstants.YCSB_MAX_RECORD_PER_PART + 1;
			generateYcsbTable(startId, TpccConstants.YCSB_RECORD_PER_PART);
//		}
	}
	
	private void generateYcsbTable(int startId, int recordCount) {
		int endId = startId + recordCount - 1;
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Start populating YCSB table from i_id=" + startId
					+ " to i_id=" + endId + " (count = " + recordCount + ")");
		
		// Generate the field names of YCSB table
		String sqlPrefix = "INSERT INTO ycsb (ycsb_id";
		for (int count = 1; count < TpccConstants.YCSB_FIELD_COUNT; count++) {
			sqlPrefix += ", ycsb_" + count;
		}
		sqlPrefix += ") VALUES (";
		
		String sql;
		String ycsbId, ycsbValue;
		for (int id = startId, recCount = 1; id <= endId; id++, recCount++) {
			
			// The primary key of YCSB is the string format of id
			ycsbId = String.format(TpccConstants.YCSB_ID_FORMAT, id);
			
			sql = sqlPrefix + "'" + ycsbId + "'";
			
			// All values of the fields use the same value
			ycsbValue = ycsbId;
			
			for (int count = 1; count < TpccConstants.YCSB_FIELD_COUNT; count++) {
				sql += ", '" + ycsbValue + "'";
			}
			sql += ")";

			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
			
			if (recCount % 50000 == 0)
				if (logger.isLoggable(Level.INFO))
					logger.info(recCount + " YCSB records has been populated.");
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Populating YCSB table completed.");
	}
}
