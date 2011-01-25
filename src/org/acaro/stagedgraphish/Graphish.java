package org.acaro.stagedgraphish;

import org.acaro.stagedgraphish.operations.Stages;

public class Graphish {
	
	public Vertex getVertex(byte[] id){
		return Stages.getStore().addOperationGetVertex(id).get();
	}
		
	public Vertex addVertex() {
		return Stages.getStore().addOperationCreateVertex().get();
	}
	
	public Edge getEdge(byte[] id){
		return Stages.getStore().addOperationGetEdge(id).get();
	}
	
	public Iterable<Edge> getEdges(){
		return Stages.getStore().getIterableEdges();
	}
	
	public Iterable<Vertex> getVertices(){
		return Stages.getStore().getIterableVertices();
	}
}