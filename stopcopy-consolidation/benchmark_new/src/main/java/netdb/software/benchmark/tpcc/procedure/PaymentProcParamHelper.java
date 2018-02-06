package netdb.software.benchmark.tpcc.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.server.VanillaDdDb;

public class PaymentProcParamHelper extends StoredProcedureParamHelper {

	protected int wid, did, cwid, cdid, cidInt;
	protected String cDataStr;
	protected Constant cid, cLast, cMiddle, cFirst, cStreet1, cStreet2, cCity, cState, cZip, cPhone, cCredit, cSince,
			cBalance, cCreditLim, cDiscount;
	String wName, wStreet1, wStreet2, wCity, wState, wZip;

	protected long hDateLong;
	protected double hAmount;
	protected boolean isCommitted = true, isBadCredit = false;

	@Override
	public void prepareParameters(Object... pars) {
		if (pars.length != 6)
			throw new RuntimeException("wrong pars list");
		wid = (Integer) pars[0];
		did = (Integer) pars[1];
		cwid = (Integer) pars[2];
		cdid = (Integer) pars[3];
		cidInt = (Integer) pars[4];
		hAmount = (Double) pars[5];

		cid = new IntegerConstant(cidInt);
	}

	@Override
	public SpResultSet createResultSet() {
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
			rec.setVal("c_data", new VarcharConstant(cDataStr, Type.VARCHAR(200)));

		// Sender
		sch.addField("sender", Type.INTEGER);
		rec.setVal("sender", new IntegerConstant(VanillaDdDb.serverId()));
		
		return new SpResultSet(sch, rec);
	}

	public int getWid() {
		return wid;
	}

	public int getDid() {
		return did;
	}

	public int getCwid() {
		return cwid;
	}

	public int getCdid() {
		return cdid;
	}

	public double getHamount() {
		return hAmount;
	}

	public int getcidInt() {
		return cidInt;
	}

	public void setcLast(Constant x) {
		this.cLast = x;
	}

	public void setcMiddle(Constant x) {
		this.cMiddle = x;
	}

	public void setcFirst(Constant x) {
		this.cFirst = x;
	}

	public void setcStreet1(Constant x) {
		this.cStreet1 = x;
	}

	public void setcStreet2(Constant x) {
		this.cStreet2 = x;
	}

	public void setcCity(Constant x) {
		this.cCity = x;
	}

	public void setcState(Constant x) {
		this.cState = x;
	}

	public void setcZip(Constant x) {
		this.cZip = x;
	}

	public void setcPhone(Constant x) {
		this.cPhone = x;
	}

	public void setcCredit(Constant x) {
		this.cCredit = x;
	}

	public void setcSince(Constant x) {
		this.cSince = x;
	}

	public void setcBalance(Constant x) {
		this.cBalance = x;
	}

	public void setcCreditLim(Constant x) {
		this.cCreditLim = x;
	}

	public void setcDiscount(Constant x) {
		this.cDiscount = x;
	}
	public void setisBadCredit(boolean bc){
		this.isBadCredit = bc;
	}
	
	public void setcDataStr(String cDataStr){
		this.cDataStr = cDataStr;
	}
	public void sethDateLong(long hDateLong){
		this.hDateLong = hDateLong;
	}

}
