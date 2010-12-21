package org.acaro.graphish;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/*
 * XXX: should implement own hashCode based on id?
 * As Graphish creates the Vertices, there should never be two Vertex instances with same id around... 
 * TODO: add specular call for putting edge vertex.putOutgoingEdge(other) should also call other.putIncomingEdge(vertex)
 */


public class VertexImpl implements Vertex {
	// reference to adjanceny list
	private HashMap<String, HashMap<byte[], Edge>> typedIncoming = new HashMap<String, HashMap<byte[], Edge>>();
	private HashMap<String, HashMap<byte[], Edge>> typedOutgoing = new HashMap<String, HashMap<byte[], Edge>>();
	private HashMap<byte[], Edge> outgoing = new HashMap<byte[], Edge>();
	private HashMap<byte[], Edge> incoming = new HashMap<byte[], Edge>();
	private Graphish graph;
	private byte[] id;
	private int hashCode = -1;
	
	protected VertexImpl(Graphish graph, byte[] id){
		this.graph = graph;
		this.id    = id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof VertexImpl)) return false;

		VertexImpl vertex = (VertexImpl) o;

		return this.id.equals(vertex.id);
	}
	
	@Override
	public int hashCode() {
		if (hashCode == -1) // only calculate the hash code once
			hashCode = this.id.hashCode();

		return hashCode;
	}
	
	public boolean hasProperty(String key) throws IOException {
		return graph.getPropertiesStore().hasProperty(this, key);
	}

	public byte[] setProperty(String key, byte[] value) throws IOException {
		return graph.getPropertiesStore().setProperty(this, key, value);
	}

	public byte[] getProperty(String key) throws IOException {
		return graph.getPropertiesStore().getProperty(this, key);
	}

	public byte[] removeProperty(String key) throws IOException {
		return graph.getPropertiesStore().removeProperty(this, key);
	}

	public Iterable<String> getPropertyKeys() throws IOException {
		return graph.getPropertiesStore().getPropertyKeys(this);
	}

	public Iterable<byte[]> getPropertyValues() throws IOException {
		return graph.getPropertiesStore().getPropertyValues(this);
	}

	public byte[] getId() {
		return this.id;
	}

	public Edge putOutgoingEdge(Vertex other, String type) {
		Edge edge = graph.createEdge(this, other, type);
		outgoing.put(other.getId(), edge);

		HashMap<byte[], Edge> n;
		if((n = typedOutgoing.get(type)) == null){
			n = new HashMap<byte[], Edge>();
		}
		
		typedOutgoing.put(type, n);
		return n.put(other.getId(), edge);
	}

	public Iterable<Edge> getOutgoingEdges() {
		return outgoing.values();
	}

	public Iterable<Edge> getOutgoingEdges(String type) {
		HashMap<byte[], Edge> n;
		if((n = typedOutgoing.get(type)) == null){
			return new ArrayList<Edge>(); //XXX: anything better than ArrayList for an empty List?
		}
		
		return n.values();
	}
	
	public Edge putIncomingEdge(Vertex other, String type) {
		Edge edge = graph.createEdge(this, other, type);
		incoming.put(other.getId(), edge);

		HashMap<byte[], Edge> n;
		if((n = typedIncoming.get(type)) == null){
			n = new HashMap<byte[], Edge>();
		}
		
		typedIncoming.put(type, n);
		return n.put(other.getId(), edge);
	}

	public Iterable<Edge> getIncomingEdges() {
		return incoming.values();
	}

	public Iterable<Edge> getIncomingEdges(String type) {
		HashMap<byte[], Edge> n;
		if((n = typedIncoming.get(type)) == null){
			return new ArrayList<Edge>(); //XXX: anything better than ArrayList for an empty List?
		}
		
		return n.values();
	}
}