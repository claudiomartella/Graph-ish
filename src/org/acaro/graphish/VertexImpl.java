package org.acaro.graphish;

public class VertexImpl implements Vertex {
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
	
	public boolean hasProperty(String key){
		return graph.getPropertyStore().hasProperty(id, key);
	}

	public void setProperty(String key, byte[] value){
		graph.getPropertyStore().setProperty(id, key, value);
	}

	public byte[] getProperty(String key){
		return graph.getPropertyStore().getProperty(id, key);
	}

	public byte[] removeProperty(String key){
		return graph.getPropertyStore().removeProperty(id, key);
	}

	public Iterable<String> getPropertyKeys(){
		return graph.getPropertyStore().getPropertyKeys(id);
	}

	public Iterable<byte[]> getPropertyValues(){
		return graph.getPropertyStore().getPropertyValues(id);
	}

	public byte[] getId() {
		return this.id;
	}

	public Edge putOutgoingEdge(Vertex other, String type) {
		return graph.getGraphStore().putOutgoingEdge(this, other, type);
	}

	public Iterable<Edge> getOutgoingEdges() {
		return graph.getGraphStore().getOutgoingEdges(this);
	}

	public Iterable<Edge> getOutgoingEdges(String type) {
		return graph.getGraphStore().getOutgoingEdges(this, type);
	}
	
	public Edge putIncomingEdge(Vertex other, String type) {
		return graph.getGraphStore().putIncomingEdge(other, this, type);
	}

	public Iterable<Edge> getIncomingEdges() {
		return graph.getGraphStore().getIncomingEdges(this);
	}

	public Iterable<Edge> getIncomingEdges(String type) {
		return graph.getGraphStore().getIncomingEdges(this, type);
	}
}