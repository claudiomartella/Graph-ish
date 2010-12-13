package org.acaro.graphish;

import java.util.ArrayList;
import java.util.HashMap;

/*
 * XXX: should implement a better hashCode based on id?
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
	private GraphStore store;
	private byte[] id;
	
	protected VertexImpl(Graphish graph, byte[] id){
		this.graph = graph;
		this.id    = id;
	}
	
	public boolean hasProperty(String key) {
		return graph.getStore().hasProperty(this, key);
	}

	public byte[] setProperty(String key, byte[] value) {
		return graph.getStore().setProperty(this, key, value);
	}

	public byte[] getProperty(String key) {
		return graph.getStore().getProperty(this, key);
	}

	public byte[] removeProperty(String key) {
		return graph.getStore().removeProperty(this, key);
	}

	public Iterable<String> getPropertyKeys() {
		return graph.getStore().getPropertyKeys(this);
	}

	public Iterable<byte[]> getPropertyValues() {
		return graph.getStore().getPropertyValues(this);
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