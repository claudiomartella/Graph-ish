package org.acaro.graphish;

import java.io.IOException;
import java.util.HashMap;

/*
 * TODO: add the code for id generation. Ideally UUID.randomUUID().toBytesArray()
 */

public class Graphish  {
	private GraphStore store;
	private HashMap<byte[], Vertex> vertices = new HashMap<byte[], Vertex>();
	
	public Graphish(GraphStore store) throws IOException {
		this.store = store;
	}

	public Edge createEdge(Vertex from, Vertex to, String type) {
		byte[] id;
		// should create id from UUID
		Edge e = new EdgeImpl(this, id, from, to, type);
		from.putOutgoingEdge(to, type);
		
		// persist? store...
		
		return e;
	}
	
	public Vertex getVertex(byte[] id) {
		return vertices.get(id);
	}
	
	public Vertex createVertex() {
		byte[] id;
		
		Vertex v = new VertexImpl(this, id);
		vertices.put(id, v);
		
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