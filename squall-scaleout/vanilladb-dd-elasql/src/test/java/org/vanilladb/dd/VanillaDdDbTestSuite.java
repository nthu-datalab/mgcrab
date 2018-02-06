package org.vanilladb.dd;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.vanilladb.dd.IsolatedClassLoaderSuite.IsolationRoot;
import org.vanilladb.dd.server.ServerInit;
import org.vanilladb.dd.server.VanillaDdDb;

@RunWith(IsolatedClassLoaderSuite.class)
@IsolationRoot(VanillaDdDb.class)
public class VanillaDdDbTestSuite {
	@BeforeClass
	public static void init() {
		// delete old test bed
		String homedir = System.getProperty("user.home");
		File oldDbDirectory = new File(homedir, ServerInit.dbName);
		if (oldDbDirectory.exists()) {
			for (String filename : oldDbDirectory.list())
				new File(oldDbDirectory, filename).delete();
			oldDbDirectory.delete();
		}
	}
}
