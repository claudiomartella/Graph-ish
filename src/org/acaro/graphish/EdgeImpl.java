package org.acaro.graphish;

public class EdgeImpl implements Edge {
	private Vertex from, to;
	private Graphish graph;
	private String type;
	private byte[] id;
	private int hashCode = -1;
	
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
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof EdgeImpl)) return false;

		EdgeImpl edge = (EdgeImpl) o;

		return this.id.equals(edge.id);
	}
	
	@Override
	public int hashCode() {
		if (hashCode == -1) // only calculate the hash code once
			hashCode = this.id.hashCode();

		return hashCode;
	}

	public boolean hasProperty(String key) {
		return graph.getPropertyStore().hasProperty(id, key);
	}

	public void setProperty(String key, byte[] value) {
		graph.getPropertyStore().setProperty(id, key, value);
	}

	public byte[] getProperty(String key) {
		return graph.getPropertyStore().getProperty(id, key);
	}

	public byte[] removeProperty(String key) {
		return graph.getPropertyStore().removeProperty(id, key);
	}

	public Iterable<String> getPropertyKeys() {
		return graph.getPropertyStore().getPropertyKeys(id);
	}

	public Iterable<byte[]> getPropertyValues() {
		return graph.getPropertyStore().getPropertyValues(id);
	}

	public Vertex getFrom() {
		return from;
	}

	public Vertex getTo() {
		return to;
	}

	public Vertex getOther(Vertex vertex) {
		if(vertex.equals(this.to)){
			return this.from;
		} else if(vertex.equals(this.from)) {
			return this.to;
		} else {
			return null;
		}
	}

	public String getType() {
		return type;
	}
}