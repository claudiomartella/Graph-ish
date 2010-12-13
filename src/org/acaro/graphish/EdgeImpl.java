package org.acaro.graphish;

public class EdgeImpl implements Edge {
	private Vertex from, to;
	private Graphish graph;
	private String type;
	private byte[] id;
	
	protected EdgeImpl(Graphish graph, byte[] id, Vertex from, Vertex to, String type){
		this.graph = graph;
		this.type  = type;
		this.from  = from;
		this.to = to;
		this.id = id;
	}
	
	public byte[] getId() {
		return id;
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

	public Vertex getFrom() {
		return from;
	}

	public Vertex getTo() {
		return to;
	}
	/*
	 	XXX: should check Vertex.equals() instead?
	 */
	public Vertex getOther(Vertex vertex) {
		if(vertex == this.to){
			return this.from;
		} else if(vertex == this.from) {
			return this.to;
		} else {
			return null;
		}
	}

	public String getType() {
		return type;
	}
}