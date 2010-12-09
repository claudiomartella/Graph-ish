package org.acaro.graphish;

import no.ntnu.idi.sgdb.SGDB;
import no.ntnu.idi.sgdb.SGDBException;
import no.ntnu.idi.sgdb.service.SgdbPersistentBDBServiceImple;

public class GraphishMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Welcome to Graphandra v0.1");
		String path = "./bdb";
		try {
			SgdbPersistentBDBServiceImple s = (SgdbPersistentBDBServiceImple) SGDB
					.createPersistentSgdbService(path, false, SGDB.BDB);
		} catch (SGDBException e) {
			e.printStackTrace();
		}
	}

}
