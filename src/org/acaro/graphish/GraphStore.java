package org.acaro.graphish;

import java.io.IOException;

public interface GraphStore {

	public Vertex createVertex(Graphish graph) throws IOException;
	
	public Edge createEdge(Graphish graph, Vertex from, Vertex to, String type) throws IOException;
	
	public Edge putOutgoingEdge(Vertex from, Vertex to, String type) throws IOException;
	
	public Iterable<Edge> getOutgoingEdges(Vertex vertex);
	
	public Iterable<Edge> getOutgoingEdges(Vertex vertex, String type);
	
	public Edge putIncomingEdge(Vertex from, Vertex to, String type) throws IOException;
	
	public Iterable<Edge> getIncomingEdges(Vertex vertex);
	
	public Iterable<Edge> getIncomingEdges(Vertex vertex, String type);
}
