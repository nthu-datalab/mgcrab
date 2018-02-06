package org.vanilladb.core.sql.storedprocedure;

public interface StoredProcedureFactory {
	
	StoredProcedure getStroredProcedure(int pid);

//	public static StoredProcedure getStoredProcedure(int pid) {
//		StoredProcedure sp;
//		switch (pid) {
//		case 1:
//			sp = new SchemaBuilder();
//			break;
//		case 2:
//			sp = new TestbedLoader();
//			break;
//		case 3:
//			sp = new NewOrderTransaction();
//			break;
//		default:
//			throw new UnsupportedOperationException();
//		}
//		return sp;
//	}
}
