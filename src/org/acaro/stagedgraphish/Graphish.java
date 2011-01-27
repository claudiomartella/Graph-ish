package org.acaro.stagedgraphish;

import java.util.concurrent.Future;

import org.acaro.stagedgraphish.operations.Stages;

public class Graphish {
	
	public Future<Vertex> getVertex(byte[] id){
		if(id == null) throw new IllegalArgumentException("argument is null");
		
		return Stages.getStore().addOperationGetVertex(id);
	}
		
	public Future<Vertex> addVertex(){
		return Stages.getStore().addOperationCreateVertex();
	}
	
	public Future<Edge> getEdge(byte[] id){
		if(id == null) throw new IllegalArgumentException("argument is null");
		
		return Stages.getStore().addOperationGetEdge(id);
	}
	
	public Iterable<Edge> getEdges(){
		return Stages.getStore().getIterableEdges();
	}
	
	public Iterable<Vertex> getVertices(){
		return Stages.getStore().getIterableVertices();
	}
}