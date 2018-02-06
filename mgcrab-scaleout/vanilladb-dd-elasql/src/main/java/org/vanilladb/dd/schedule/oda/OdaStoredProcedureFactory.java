package org.vanilladb.dd.schedule.oda;

import org.vanilladb.dd.schedule.DdStoredProcedureFactory;

public interface OdaStoredProcedureFactory extends DdStoredProcedureFactory {
	
	@Override
	OdaStoredProcedure getStoredProcedure(int pid, long txNum);
	
}
