package org.acaro.stagedgraphish;

import java.io.IOException;

public class GraphishMain {

	public static void main(String[] args) throws IOException {
		System.out.println("Welcome to Graph-ish v0.1");
		GraphStore gStore = new OldHBaseStore();
		PropertyStore pStore = (PropertyStore) gStore;
		Graphish graph = new Graphish(pStore, gStore);
	}
}
