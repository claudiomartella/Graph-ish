package org.acaro.stagedgraphish;

public class EdgeExists extends RuntimeException {
	private static final long serialVersionUID = 4409616261093562533L;

	public EdgeExists(Vertex from, Vertex to, String type){
		super("Edge " + type + " connecting " + from + " to " + to + " already exists");
	}
}
