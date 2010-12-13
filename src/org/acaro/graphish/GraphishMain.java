package org.acaro.graphish;

public class GraphishMain {

	public static void main(String[] args) {
		System.out.println("Welcome to Graph-ish v0.1");
		Graphish graph = new Graphish(new HBaseStore());
	}
}
