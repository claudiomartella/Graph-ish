package org.acaro.graphish;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class Graphish  {
	private GraphStore store;
	
	public Graphish(GraphStore store) throws IOException {
		this.store = store;
	}

	public Vertex addVertex() {
		return store.createVertex();
	}
	
	public Vertex getVertex() {
		return null;
	}

	public void removeVertex(Vertex v) {
		store.removeVertex(v);
	}

	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label){
		return null;
	}
	
	public Edge getEdge(Object id) {
		return null;
	}
	
	public void removeEdge(Edge edge) {
	}
	// sync, flush and close connections
	public void shutdown() {
		store.shutdown();
	}
	// delete the graph from storage
	public void clear() {
		store.clear();
	}

	public Iterable<Edge> getEdges() {
		return null;
	}
		
	public Iterable<Vertex> getVertices() {
		return null;
	}
}