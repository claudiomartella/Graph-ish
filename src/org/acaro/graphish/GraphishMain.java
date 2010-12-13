package org.acaro.graphish;

import java.io.IOException;

public class GraphishMain {

	public static void main(String[] args) throws IOException {
		System.out.println("Welcome to Graph-ish v0.1");
		Graphish graph = new Graphish(new HBaseStore("GraphishTest", false));
	}
}
