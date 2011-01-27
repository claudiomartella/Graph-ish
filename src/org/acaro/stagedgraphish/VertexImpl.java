package org.acaro.stagedgraphish;

import java.util.concurrent.Future;

import org.acaro.stagedgraphish.operations.Stages;

public class VertexImpl implements Vertex {
	private byte[] id;
	private int hashCode = -1;
	
	public VertexImpl(byte[] id){
		this.id = id;
	}

	@Override
	public boolean equals(Object o){
		if (this == o) return true;
		if (!(o instanceof VertexImpl)) return false;

		VertexImpl vertex = (VertexImpl) o;

		return this.id.equals(vertex.id);
	}
	
	@Override
	public int hashCode(){
		if (hashCode == -1){
			hashCode = this.id.hashCode();
		}
		
		return hashCode;
	}
	
	public byte[] getId(){
		return this.id;
	}
	
	public Future<Boolean> hasProperty(String key){
		if(key == null) throw new IllegalArgumentException("argument is null");
		
		return Stages.getStore().addOperationVertexHasProperty(this, key);
	}
	
	public Future<Void> setProperty(String key, byte[] value){
		if(key == null || value == null) throw new IllegalArgumentException("argument is null");
	
		return Stages.getStore().addOperationVertexSetProperty(this, key, value);
	}

	public Future<byte[]> getProperty(String key){
		if(key == null) throw new IllegalArgumentException("argument is null");

		return Stages.getStore().addOperationVertexGetProperty(this, key);
	}

	public Future<byte[]> removeProperty(String key){
		if(key == null) return null;

		return Stages.getStore().addOperationVertexRemoveProperty(this, key);
	}

	public Iterable<String> getPropertyKeys(){
		return Stages.getStore().getIterableVertexPropertyKeys(this);
	}

	public Iterable<byte[]> getPropertyValues(){
		return Stages.getStore().getIterableVertexPropertyValues(this);
	}

	public Future<Edge> putOutgoingEdge(Vertex other, String type){
		if(other == null || type == null) return null;

		return Stages.getStore().addOperationVertexPutOutgoingEdge(this, other, type);
	}

	public Iterable<Edge> getOutgoingEdges(){
		return Stages.getStore().getIterableVertexOutgoingEdges(this, null);
	}

	public Iterable<Edge> getOutgoingEdges(String type){
		return Stages.getStore().getIterableVertexOutgoingEdges(this, type);
	}
	
	public Future<Edge> putIncomingEdge(Vertex other, String type){
		if(other == null || type == null) throw new IllegalArgumentException("argument is null");
		
		return Stages.getStore().addOperationVertexPutIncomingEdge(this, other, type);
	}

	public Iterable<Edge> getIncomingEdges(){
		return Stages.getStore().getIterableVertexIncomingEdges(this, null);
	}

	public Iterable<Edge> getIncomingEdges(String type){
		return Stages.getStore().getIterableVertexIncomingEdges(this, type);
	}

	public Future<Void> delete() {
		return Stages.getStore().addOperationRemoveVertex(this);
	}
}