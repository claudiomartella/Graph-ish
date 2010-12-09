package org.acaro.graphish;

public interface Vertex extends PropertyContainer {
	
	public byte[] getId();
	
	public Edge putOutgoingEdge(Vertex other, String type);
	
	public Iterable<Edge> getOutgoingEdges();
	
	public Iterable<Edge> getOutgoingEdges(String type);
	
	public Edge putIncomingEdge(Vertex other, String type);
	
	public Iterable<Edge> getIncomingEdges();
	
	public Iterable<Edge> getIncomingEdges(String type);
}
