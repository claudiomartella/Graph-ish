package org.acaro.graphish;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

/*
 * 
 */

public class Graphish  {
	private PropertyStore pStore;
	private GraphStore gStore;
	
	public Graphish(PropertyStore pStore, GraphStore gStore) throws IOException {
		this.pStore = pStore;
		this.gStore = gStore;
	}

	public Edge createEdge(Vertex from, Vertex to, String type) {
		
		Edge e = gStore.createEdge(this, from, to, type);
		from.putOutgoingEdge(to, type);
		
		return e;
	}
	
	public Vertex getVertex(byte[] id) {
		return vertices.get(id);
	}
	
	public Vertex createVertex() {
		
		Vertex v = gStore.createVertex(this);
		
		return v;
	}
	
	public Vertex removeVertex(byte[] id) {
		Vertex v = getVertex(id);
		
		// v.die(); should remove itself from neighborhood's adj and persistence layer
		return v;
	}
	
	protected PropertyStore getPropertiesStore() {
		return this.pStore;
	}
	
	protected GraphStore getGraphStore() {
		return this.gStore;
	}
}