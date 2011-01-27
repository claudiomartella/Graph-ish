package org.acaro.stagedgraphish;

import java.util.concurrent.Future;

public interface Vertex extends PropertyContainer {
	
	public Future<Edge> putOutgoingEdge(Vertex other, String type);
	
	public Iterable<Edge> getOutgoingEdges();
	
	public Iterable<Edge> getOutgoingEdges(String type);
	
	public Future<Edge> putIncomingEdge(Vertex other, String type);
	
	public Iterable<Edge> getIncomingEdges();
	
	public Iterable<Edge> getIncomingEdges(String type);

	public Future<Void> delete();
}
