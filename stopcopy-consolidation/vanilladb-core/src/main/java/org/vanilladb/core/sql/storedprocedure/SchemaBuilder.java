package org.vanilladb.core.sql.storedprocedure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Example stored procedure.
 */
public class SchemaBuilder implements StoredProcedure {
	private static Logger logger = Logger.getLogger(SchemaBuilder.class
			.getName());
	private final String TPCC_TABLES_DDL[] = {
			"CREATE TABLE Warehouse ( " + "wh_id INT ,"
					+ "wh_name VARCHAR(10) ," + "wh_address VARCHAR(30) ,"
					+ "wh_tax_rate DOUBLE ," + "wh_year_to_date_balance INT )",
			"CREATE TABLE District ( " + "dt_id INT ,"
					+ "dt_warehouse_id INT ," + "dt_name VARCHAR(10) ,"
					+ "dt_address VARCHAR(30) ," + "dt_tax_rate DOUBLE ,"
					+ "dt_year_to_date_balance INT ,"
					+ "dt_next_order_id INT )",
			"CREATE TABLE Item ( " + "item_id INT ," + "item_image_id INT,"
					+ "item_name VARCHAR(10) ," + "item_price INT ,"
					+ "item_brand_info VARCHAR(30) )",
			"CREATE TABLE Stock ( " + "st_warehouse_id INT ,"
					+ "st_item_id INT ," + "st_quantity_num INT ,"
					+ "st_year_to_date_balance INT ," + "st_order_count INT )",
			"CREATE TABLE Customer ( " + "ctm_district_id INT ,"
					+ "ctm_warehouse_id INT ," + "ctm_id INT ,"
					+ "ctm_name VARCHAR(10) ," + "ctm_address VARCHAR(30) ,"
					+ "ctm_phone_num VARCHAR(20) ,"
					+ "ctm_register_date LONG ," + "ctm_discount_rate DOUBLE ,"
					+ "ctm_balance INT ," + "ctm_year_date_payment INT ,"
					+ "ctm_delivery_count INT )",
			"CREATE TABLE Orders ( " + "ord_ctm_district_id INT ,"
					+ "ord_ctm_warehouse_id INT ," + "ord_ctm_id INT ,"
					+ "ord_id INT ," + "ord_entry_date LONG ,"
					+ "ord_carrier_id INT ," + "ord_order_line_count INT ,"
					+ "ord_all_local INT )",
			"CREATE TABLE Order_Line ( " + "ol_id INT ,"
					+ "ol_warehouse_id INT ," + "ol_item_id INT ,"
					+ "ol_ctm_id INT ," + "ol_ctm_district_id INT ,"
					+ "ol_ctm_warehouse_id INT ," + "ol_ord_id INT ,"
					+ "ol_delivery_date LONG ," + "ol_amount INT ,"
					+ "ol_quantity INT )",
			"CREATE TABLE Order_History ( " + "oh_id INT ,"
					+ "oh_ctm_district_id INT ," + "oh_ctm_warehouse_id INT ,"
					+ "oh_ctm_id INT ," + "oh_paid_district_id INT ,"
					+ "oh_paid_warehouse_id INT ," + "oh_date LONG,"
					+ "oh_amount INT )",
			"CREATE TABLE New_Order ( " + "no_ctm_district_id INT ,"
					+ "no_ctm_warehouse_id INT ," + "no_ctm_id INT ,"
					+ "no_ord_id INT )" };

	private long txNum;
	private StringBuilder outMsg;

	public SchemaBuilder() {
		this.outMsg = new StringBuilder();
	}

	@Override
	public void prepare(Object... pars) {
		// no pars
	}

	@Override
	public SpResultSet execute() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Execute proc: schema builder...");

		// Step 1: Create new transaction
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false, txNum);

		// Step 2: Declare the read/write set
		// String tcatFileName = TableMgr.TCAT + ".tbl";
		// String fcatFileName = TableMgr.FCAT + ".tbl";
		// Object[] writeSet = { tcatFileName, fcatFileName };

		// Step 3: Conservative ordered locking

		/*
		 * TODO Modify the concurrency manager and lock table to provide method
		 * to conservatively lock read/write set.
		 */

		// Step 4: Execute stored procedure logic
		try {
			// Create schema
			int result;
			for (String cmd : TPCC_TABLES_DDL) {
				result = VanillaDb.planner().executeUpdate(cmd, tx);
				if (result < 0)
					throw new SQLException("failed to create table");
			}
			tx.commit();
			outMsg.append("commited. schema created.");

		} catch (Exception e) {
			tx.rollback();
			outMsg.append("rollback.");

			e.printStackTrace();
		}
		return null;
	}
}
