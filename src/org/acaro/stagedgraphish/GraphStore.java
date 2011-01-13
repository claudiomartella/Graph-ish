package org.acaro.stagedgraphish;

public interface GraphStore {

	public Vertex createVertex();
	
	public Vertex getVertex(byte[] id);
	
	public void removeVertex(Vertex vertex);
	
	public Edge createEdge(Vertex from, Vertex to, String type);
	
	public void removeEdge(Edge edge);
	
	public Edge putOutgoingEdge(Vertex from, Vertex to, String type);
	
	public Iterable<Edge> getOutgoingEdges(Vertex vertex);
	
	public Iterable<Edge> getOutgoingEdges(Vertex vertex, String type);
	
	public Edge putIncomingEdge(Vertex from, Vertex to, String type);
	
	public Iterable<Edge> getIncomingEdges(Vertex vertex);
	
	public Iterable<Edge> getIncomingEdges(Vertex vertex, String type);
}
