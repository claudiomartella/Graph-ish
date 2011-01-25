package org.acaro.stagedgraphish;

import org.acaro.stagedgraphish.operations.Stages;

public class EdgeImpl implements Edge {
	private Vertex from, to;
	private String type;
	private byte[] id;
	private int hashCode = -1;
	
	public EdgeImpl(byte[] id, Vertex from, Vertex to, String type){
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
	
	public boolean hasProperty(String key){
		if(key == null) return false;
		
		return Stages.getStore().addOperationEdgeHasProperty(this, key).get();
	}
	
	public void setProperty(String key, byte[] value){
		if(key == null || value == null) return;
	
		Stages.getStore().addOperationEdgeSetProperty(this, key, value).get();
	}

	public byte[] getProperty(String key){
		if(key == null) return null;

		return Stages.getStore().addOperationEdgeGetProperty(this, key).get();
	}

	public byte[] removeProperty(String key){
		if(key == null) return null;

		return Stages.getStore().addOperationEdgeRemoveProperty(this, key).get();
	}

	public Iterable<String> getPropertyKeys(){
		return Stages.getStore().addOperationEdgeGetPropertyKeys(this).get();
	}

	public Iterable<byte[]> getPropertyValues(){
		return Stages.getStore().addOperationEdgeGetPropertyValues(this).get();
	}

	public void delete() {
		return Stages.getStore().addOperationRemoveEdge(this).get();
	}
}