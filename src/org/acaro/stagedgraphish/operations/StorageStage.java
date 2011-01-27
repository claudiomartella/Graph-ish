package org.acaro.stagedgraphish.operations;

import java.util.List;
import java.util.concurrent.Future;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.PropertyContainer;
import org.acaro.stagedgraphish.Vertex;

public interface StorageStage {
	
	public Future<Boolean> addOperationVertexHasProperty(Vertex v, String key);
	
	public Future<Void> addOperationVertexSetProperty(Vertex v, String key, byte[] value);

	public Future<byte[]> addOperationVertexGetProperty(Vertex v, String key);

	public Future<byte[]> addOperationVertexRemoveProperty(Vertex v, String key);

	public Future<List<String>> addOperationVertexGetPropertyKeys(Vertex v);

	public Future<List<byte[]>> addOperationVertexGetPropertyValues(Vertex v);
	
	public Future<Boolean> addOperationEdgeHasProperty(Edge e, String key);
	
	public Future<Void> addOperationEdgeSetProperty(Edge e, String key, byte[] value);

	public Future<byte[]> addOperationEdgeGetProperty(Edge e, String key);

	public Future<byte[]> addOperationEdgeRemoveProperty(Edge e, String key);

	public Future<List<String>> addOperationEdgeGetPropertyKeys(Edge e);

	public Future<List<byte[]>> addOperationEdgeGetPropertyValues(Edge e);

	public Future<Edge> addOperationVertexPutOutgoingEdge(Vertex v, Vertex other, String type);

	public Future<Edge> addOperationVertexPutIncomingEdge(Vertex v, Vertex other, String type);
	
	public Future<List<Edge>> addOperationGetNeighbors(EdgeFilter filter);
	
	public Future<List<Edge>> addOperationGetEdges(ContainerFilter filter);
	
	public Future<List<Vertex>> addOperationGetVertices(ContainerFilter filter);
	
	public Future<Vertex> addOperationCreateVertex();
	
	public Future<Vertex> addOperationGetVertex(byte[] id);
	
	public Future<Void> addOperationRemoveVertex(Vertex v);
	
	public Future<Edge> addOperationGetEdge(byte[] id);
	
	public Future<Void> addOperationRemoveEdge(Edge e);
	
	public Iterable<String> getIterableVertexPropertyKeys(Vertex v);

	public Iterable<byte[]> getIterableVertexPropertyValues(Vertex v);

	public Iterable<String> getIterableEdgePropertyKeys(Edge e);

	public Iterable<byte[]> getIterableEdgePropertyValues(Edge e);
	
	public Iterable<Edge> getIterableVertexIncomingEdges(Vertex v, String type);
	
	public Iterable<Edge> getIterableVertexIncomingEdges(Vertex v);

	public Iterable<Edge> getIterableVertexOutgoingEdges(Vertex v, String type);
	
	public Iterable<Edge> getIterableVertexOutgoingEdges(Vertex v);
	
	public Iterable<Edge> getIterableEdges();
	
	public Iterable<Vertex> getIterableVertices();
}