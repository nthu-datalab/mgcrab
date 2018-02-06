package netdb.software.benchmark.tpcc.procedure;

import netdb.software.benchmark.tpcc.util.DoublePlainPrinter;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class PaymentProcedure {
	protected Transaction tx;

	protected boolean selectByCLast;
	protected int wid, did, cwid, cdid, cidInt;
	protected String cLastStr, cDataStr;
	protected Constant cid, cLast, cMiddle, cFirst, cStreet1, cStreet2, cCity,
			cState, cZip, cPhone, cCredit, cSince, cBalance, cCreditLim,
			cDiscount;
	protected long hDateLong;
	protected double hAmount;
	protected boolean isCommitted = true, isBadCredit;

	protected void prepareParameters(Object... pars) {
		if (pars.length != 6)
			throw new RuntimeException("wrong pars list");
		wid = (Integer) pars[0];
		did = (Integer) pars[1];
		cwid = (Integer) pars[2];
		cdid = (Integer) pars[3];
		if (pars[4] instanceof String) {
			selectByCLast = true;
			cLastStr = (String) pars[4];
		} else
			cidInt = (Integer) pars[4];
		hAmount = (Double) pars[5];
	}

	protected void executeSqlByCid() {
		String wName, wStreet1, wStreet2, wCity, wState, wZip;
		double wYtd;
		String sql = "SELECT w_name, w_street_1, w_street_2, w_city, "
				+ "w_state, w_zip, w_ytd FROM warehouse WHERE w_id =" + wid;
		Plan p = VanillaDb.planner().createQueryPlan(sql, tx);
		Scan s = p.open();
		s.beforeFirst();
		if (s.next()) {
			wName = (String) s.getVal("w_name").asJavaVal();
			wStreet1 = (String) s.getVal("w_street_1").asJavaVal();
			wStreet2 = (String) s.getVal("w_street_2").asJavaVal();
			wCity = (String) s.getVal("w_city").asJavaVal();
			wState = (String) s.getVal("w_state").asJavaVal();
			wZip = (String) s.getVal("w_zip").asJavaVal();
			wYtd = (Double) s.getVal("w_ytd").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		sql = "UPDATE warehouse SET w_ytd =  "
				+ DoublePlainPrinter.toPlainString(wYtd + hAmount)
				+ " WHERE w_id =" + wid;
		int result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		String dName, dStreet1, dStreet2, dCity, dState, dZip;
		double dYtd;
		sql = "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip,"
				+ "d_ytd FROM district WHERE d_w_id = " + wid + " AND d_id = "
				+ did;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			dName = (String) s.getVal("d_name").asJavaVal();
			dStreet1 = (String) s.getVal("d_street_1").asJavaVal();
			dStreet2 = (String) s.getVal("d_street_2").asJavaVal();
			dCity = (String) s.getVal("d_city").asJavaVal();
			dState = (String) s.getVal("d_state").asJavaVal();
			dZip = (String) s.getVal("d_zip").asJavaVal();
			dYtd = (Double) s.getVal("d_ytd").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		sql = "UPDATE district SET d_ytd =  "
				+ DoublePlainPrinter.toPlainString(dYtd + hAmount)
				+ " WHERE d_w_id =" + wid + " AND d_id = " + did;
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		int cPaymentCnt;
		double cYtdPayment;

		sql = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,"
				+ " c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
				+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt FROM "
				+ "customer WHERE c_w_id ="
				+ cwid
				+ " AND c_d_id = "
				+ cdid
				+ " AND c_id = " + cidInt;

		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			cFirst = s.getVal("c_first");
			cMiddle = s.getVal("c_middle");
			cLast = s.getVal("c_last");
			cStreet1 = s.getVal("c_street_1");
			cStreet2 = s.getVal("c_street_2");
			cCity = s.getVal("c_city");
			cState = s.getVal("c_state");
			cZip = s.getVal("c_zip");
			cPhone = s.getVal("c_phone");
			cCredit = s.getVal("c_credit");
			cSince = s.getVal("c_since");
			cCreditLim = s.getVal("c_credit_lim");
			cBalance = s.getVal("c_discount");
			cDiscount = s.getVal("c_discount");
			cYtdPayment = (Double) s.getVal("c_ytd_payment").asJavaVal();
			cPaymentCnt = (Integer) s.getVal("c_payment_cnt").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();
		cid = new IntegerConstant(cidInt);

		double cBalanceDouble = (Double) cBalance.asJavaVal();
		sql = "UPDATE customer SET c_balance = "
				+ DoublePlainPrinter.toPlainString(cBalanceDouble - hAmount)
				+ ", c_ytd_payment = "
				+ DoublePlainPrinter.toPlainString(cYtdPayment + hAmount)
				+ ", c_payment_cnt = " + (cPaymentCnt + 1) + " WHERE c_w_id ="
				+ cwid + " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		String cCreditStr = (String) cCredit.asJavaVal();
		if (cCreditStr.equals("BC")) {
			isBadCredit = true;
			sql = "SELECT c_data FROM customer WHERE c_w_id = " + cwid
					+ " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
			p = VanillaDb.planner().createQueryPlan(sql, tx);
			s = p.open();
			s.beforeFirst();
			if (s.next())
				cDataStr = (String) s.getVal("c_data").asJavaVal();
			else
				throw new RuntimeException();
			s.close();
			cDataStr = cidInt + " " + cdid + " " + cwid + " " + did + " " + wid
					+ " " + hAmount + " " + cDataStr;

			if (cDataStr.length() > 500)
				cDataStr = cDataStr.substring(0, 499);

			sql = "UPDATE customer SET c_data  = '" + cDataStr
					+ "' WHERE c_w_id = " + cwid + " AND c_d_id = " + cdid
					+ " AND c_id = " + cidInt;

			result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();

			if (cDataStr.length() > 200)
				cDataStr = cDataStr.substring(0, 199);
		}
		String hData = wName + " " + dName;

		hDateLong = System.currentTimeMillis();

		sql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, "
				+ "h_w_id, h_date, h_amount, h_data) VALUES (" + cidInt + ", "
				+ cdid + ", " + cwid + ", " + did + ", " + wid + ", "
				+ hDateLong + ", " + DoublePlainPrinter.toPlainString(hAmount)
				+ ", '" + hData + "')";
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();
	}

	protected void executeSqlByCLast() {
		String wName, wStreet1, wStreet2, wCity, wState, wZip;
		double wYtd;
		String sql = "SELECT w_name, w_street_1, w_street_2, w_city, "
				+ "w_state, w_zip, w_ytd FROM warehouse WHERE w_id =" + wid;
		Plan p = VanillaDb.planner().createQueryPlan(sql, tx);
		Scan s = p.open();
		s.beforeFirst();
		if (s.next()) {
			wName = (String) s.getVal("w_name").asJavaVal();
			wStreet1 = (String) s.getVal("w_street_1").asJavaVal();
			wStreet2 = (String) s.getVal("w_street_2").asJavaVal();
			wCity = (String) s.getVal("w_city").asJavaVal();
			wState = (String) s.getVal("w_state").asJavaVal();
			wZip = (String) s.getVal("w_zip").asJavaVal();
			wYtd = (Double) s.getVal("w_ytd").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		sql = "UPDATE warehouse SET w_ytd =  "
				+ DoublePlainPrinter.toPlainString(wYtd + hAmount)
				+ " WHERE w_id =" + wid;
		int result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		String dName, dStreet1, dStreet2, dCity, dState, dZip;
		double dYtd;
		sql = "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip,"
				+ "d_ytd FROM district WHERE d_w_id = " + wid + " AND d_id = "
				+ did;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			dName = (String) s.getVal("d_name").asJavaVal();
			dStreet1 = (String) s.getVal("d_street_1").asJavaVal();
			dStreet2 = (String) s.getVal("d_street_2").asJavaVal();
			dCity = (String) s.getVal("d_city").asJavaVal();
			dState = (String) s.getVal("d_state").asJavaVal();
			dZip = (String) s.getVal("d_zip").asJavaVal();
			dYtd = (Double) s.getVal("d_ytd").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		sql = "UPDATE district SET d_ytd =  "
				+ DoublePlainPrinter.toPlainString(dYtd + hAmount)
				+ " WHERE d_w_id =" + wid + " AND d_id = " + did;
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		long cIdCount;
		sql = "SELECT COUNT(c_id) FROM customer WHERE c_w_id = " + cwid
				+ " AND c_d_id = " + cdid + " AND c_last = '" + cLastStr + "'";
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();

		if (s.next())
			cIdCount = (Integer) s.getVal("countofc_id").asJavaVal();
		else
			throw new RuntimeException("last name '" + cLastStr
					+ "' is not existed");

		s.close();
		if (cIdCount % 2 == 1)
			cIdCount++;

		int cPaymentCnt = 0;
		double cYtdPayment = 0;

		sql = "SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city,"
				+ " c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
				+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt FROM "
				+ "customer WHERE c_w_id =" + cwid + " AND c_d_id = " + cdid
				+ " AND c_last = '" + cLastStr + "'";

		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		int t = 1;
		boolean isSelected = false;
		while (s.next()) {
			if (t == cIdCount / 2) {
				cFirst = s.getVal("c_first");
				cMiddle = s.getVal("c_middle");
				cid = s.getVal("c_id");
				cidInt = (Integer) cid.asJavaVal();
				cStreet1 = s.getVal("c_street_1");
				cStreet2 = s.getVal("c_street_2");
				cCity = s.getVal("c_city");
				cState = s.getVal("c_state");
				cZip = s.getVal("c_zip");
				cPhone = s.getVal("c_phone");
				cCredit = s.getVal("c_credit");
				cSince = s.getVal("c_since");
				cCreditLim = s.getVal("c_credit_lim");
				cBalance = s.getVal("c_balance");
				cDiscount = s.getVal("c_discount");
				cYtdPayment = (Double) s.getVal("c_ytd_payment").asJavaVal();
				cPaymentCnt = (Integer) s.getVal("c_payment_cnt").asJavaVal();
				isSelected = true;
				break;
			}
			t++;
		}
		s.close();
		if (!isSelected)
			throw new RuntimeException();
		cLast = new VarcharConstant(cLastStr, cFirst.getType());

		double cBalanceDouble = (Double) cBalance.asJavaVal();
		sql = "UPDATE customer SET c_balance = "
				+ DoublePlainPrinter.toPlainString(cBalanceDouble - hAmount)
				+ ", c_ytd_payment = "
				+ DoublePlainPrinter.toPlainString(cYtdPayment + hAmount)
				+ ", c_payment_cnt = " + (cPaymentCnt + 1) + " WHERE c_w_id ="
				+ cwid + " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		String cCreditStr = (String) cCredit.asJavaVal();
		if (cCreditStr.equals("BC")) {
			isBadCredit = true;
			sql = "SELECT c_data FROM customer WHERE c_w_id = " + cwid
					+ " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
			p = VanillaDb.planner().createQueryPlan(sql, tx);
			s = p.open();
			s.beforeFirst();
			if (s.next())
				cDataStr = (String) s.getVal("c_data").asJavaVal();
			else
				throw new RuntimeException();
			s.close();
			cDataStr = cidInt + " " + cdid + " " + cwid + " " + did + " " + wid
					+ " " + hAmount + " " + cDataStr;
			if (cDataStr.length() > 500)
				cDataStr = cDataStr.substring(0, 499);

			sql = "UPDATE customer SET c_data  = '" + cDataStr
					+ "' WHERE c_w_id = " + cwid + " AND c_d_id = " + cdid
					+ " AND c_id = " + cidInt;

			result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();

			if (cDataStr.length() > 200)
				cDataStr = cDataStr.substring(0, 199);
		}
		String hData = wName + " " + dName;

		hDateLong = System.currentTimeMillis();

		sql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, "
				+ "h_w_id, h_date, h_amount, h_data) VALUES (" + cidInt + ", "
				+ cdid + ", " + cwid + ", " + did + ", " + wid + ", "
				+ hDateLong + ", " + DoublePlainPrinter.toPlainString(hAmount)
				+ ", '" + hData + "')";
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();
	}

	protected SpResultSet createResultSet() {
		/*
		 * TODO The output information is not strictly followed the TPC-C
		 * definition. See the session 2.5.3.4 in TPC-C 5.11 document.
		 */
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);
		sch.addField("cid", cid.getType());
		sch.addField("c_first", cFirst.getType());
		sch.addField("c_last", cLast.getType());
		sch.addField("c_middle", cMiddle.getType());
		sch.addField("c_street_1", cStreet1.getType());
		sch.addField("c_street_2", cStreet2.getType());
		sch.addField("c_city", cCity.getType());
		sch.addField("c_state", cState.getType());
		sch.addField("c_zip", cZip.getType());
		sch.addField("c_phone", cPhone.getType());
		sch.addField("c_credit", cCredit.getType());
		sch.addField("c_since", cSince.getType());
		sch.addField("c_balance", cBalance.getType());
		sch.addField("c_credit_lim", cCreditLim.getType());
		sch.addField("c_discount", cDiscount.getType());
		sch.addField("h_date", Type.BIGINT);
		if (isBadCredit)
			sch.addField("c_data", Type.VARCHAR(200));

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		rec.setVal("cid", cid);
		rec.setVal("c_first", cFirst);
		rec.setVal("c_last", cLast);
		rec.setVal("c_middle", cMiddle);
		rec.setVal("c_street_1", cStreet1);
		rec.setVal("c_street_2", cStreet2);
		rec.setVal("c_city", cCity);
		rec.setVal("c_state", cState);
		rec.setVal("c_zip", cZip);
		rec.setVal("c_phone", cPhone);
		rec.setVal("c_credit", cCredit);
		rec.setVal("c_since", cSince);
		rec.setVal("c_balance", cBalance);
		rec.setVal("c_credit_lim", cCreditLim);
		rec.setVal("c_discount", cDiscount);
		rec.setVal("h_date", new BigIntConstant(hDateLong));
		if (isBadCredit)
			rec.setVal("c_data",
					new VarcharConstant(cDataStr, Type.VARCHAR(200)));
		return new SpResultSet(sch, rec);
	}
}
