package org.acaro.stagedgraphish;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

public class Graphish {
	private PropertyStore pStore;
	private GraphStore gStore;
	
	public Graphish(PropertyStore pStore, GraphStore gStore) throws IOException {
		this.pStore = pStore;
		this.gStore = gStore;
	}

	public Edge createEdge(Vertex from, Vertex to, String type) {
		return gStore.createEdge(from, to, type);
	}
	
	public void removeEdge(Edge edge) {
		gStore.removeEdge(edge);
	}
	
	public Vertex getVertex(byte[] id) {
		return gStore.getVertex(id);
	}
	
	public Vertex addVertex() {
		return gStore.createVertex();
	}
	
	public void removeVertex(Vertex vertex) {
		gStore.removeVertex(vertex);
	}
	
	public Iterable<Edge> getEdges() {
		return gStore.getEdges();	
	}
	
	public Iterable<Vertex> getVertices() {
		return gStore.getVertices();
	}
	
	protected PropertyStore getPropertyStore() {
		return this.pStore;
	}
	
	protected GraphStore getGraphStore() {
		return this.gStore;
	}
}