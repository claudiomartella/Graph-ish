package org.acaro.graphish;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

/*
 * 
 */

public class Graphish  {
	private GraphStore store;
	
	public Graphish(GraphStore store) throws IOException {
		this.store = store;
	}

	public Edge createEdge(Vertex from, Vertex to, String type) {
		
		Edge e = store.createEdge(this, from, to, type);
		from.putOutgoingEdge(to, type);
		
		return e;
	}
	
	public Vertex getVertex(byte[] id) {
		return vertices.get(id);
	}
	
	public Vertex createVertex() {
		
		Vertex v = store.createVertex(this);
		
		return v;
	}
	
	public Vertex removeVertex(byte[] id) {
		Vertex v = getVertex(id);
		
		// v.die(); should remove itself from neighborhood's adj and persistence layer
		return v;
	}
	
	protected GraphStore getStore(){
		return this.store;
	}
}