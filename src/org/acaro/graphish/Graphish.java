package org.acaro.graphish;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

/*
 * TODO: edges' id shouldn't be UUID, GraphStore should handle that
 */

public class Graphish  {
	private GraphStore store;
	private HashMap<byte[], Vertex> vertices = new HashMap<byte[], Vertex>();
	
	public Graphish(GraphStore store) throws IOException {
		this.store = store;
	}

	public Edge createEdge(Vertex from, Vertex to, String type) {
		UUID id = UUID.randomUUID();
		
		Edge e = new EdgeImpl(this, Bytes.fromUuid(id).toByteArray(), from, to, type);
		from.putOutgoingEdge(to, type);
		
		// persist? store...
		
		return e;
	}
	
	public Vertex getVertex(byte[] id) {
		return vertices.get(id);
	}
	
	public Vertex createVertex() {
		UUID id = UUID.randomUUID();
		
		Vertex v = new VertexImpl(this, Bytes.fromUuid(id).toByteArray());
		vertices.put(v.getId(), v);
		
		// persist? store...
		
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