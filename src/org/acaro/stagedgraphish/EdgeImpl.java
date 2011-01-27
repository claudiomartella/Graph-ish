package org.acaro.stagedgraphish;

import java.util.concurrent.Future;

import org.acaro.stagedgraphish.operations.Stages;

public class EdgeImpl implements Edge {
	private Vertex from, to;
	private String type;
	private byte[] id;
	private int hashCode = -1;
	
	public EdgeImpl(byte[] id, Vertex from, Vertex to, String type) {
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
		if(hashCode == -1){
			hashCode = this.id.hashCode();
		}
		
		return hashCode;
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
	
	public Future<Boolean> hasProperty(String key) {
		if(key == null) throw new IllegalArgumentException("argument is null");
		
		return Stages.getStore().addOperationEdgeHasProperty(this, key);
	}
	
	public Future<Void> setProperty(String key, byte[] value) {
		if(key == null || value == null) throw new IllegalArgumentException("argument is null");
	
		return Stages.getStore().addOperationEdgeSetProperty(this, key, value);
	}

	public Future<byte[]> getProperty(String key) {
		if(key == null) throw new IllegalArgumentException("argument is null");

		return Stages.getStore().addOperationEdgeGetProperty(this, key);
	}

	public Future<byte[]> removeProperty(String key) {
		if(key == null) throw new IllegalArgumentException("argument is null");

		return Stages.getStore().addOperationEdgeRemoveProperty(this, key);
	}

	public Iterable<String> getPropertyKeys() {
		return Stages.getStore().getIterableEdgePropertyKeys(this);
	}

	public Iterable<byte[]> getPropertyValues() {
		return Stages.getStore().getIterableEdgePropertyValues(this);
	}

	public Future<Void> delete() {
		return Stages.getStore().addOperationRemoveEdge(this);
	}
}