package org.acaro.stagedgraphish.operations;

import java.util.concurrent.Future;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.Vertex;

public interface StorageStage {
	
	public Future<Boolean> addOperationVertexHasProperty(Vertex v, String key);
	
	public Future<Void> addOperationVertexSetProperty(Vertex v, String key, byte[] value);

	public Future<byte[]> addOperationVertexGetProperty(Vertex v, String key);

	public Future<byte[]> addOperationVertexRemoveProperty(Vertex v, String key);

	public Future<Iterable<String>> addOperationVertexGetPropertyKeys(Vertex v);

	public Future<Iterable<byte[]>> addOperationVertexGetPropertyValues(Vertex v);
	
	public Future<Boolean> addOperationEdgeHasProperty(Edge e, String key);
	
	public Future<Void> addOperationEdgeSetProperty(Edge e, String key, byte[] value);

	public Future<byte[]> addOperationEdgeGetProperty(Edge e, String key);

	public Future<byte[]> addOperationEdgeRemoveProperty(Edge e, String key);

	public Future<Iterable<String>> addOperationEdgeGetPropertyKeys(Edge e);

	public Future<Iterable<byte[]>> addOperationEdgeGetPropertyValues(Edge e);

	public Future<Edge> addOperationVertexPutOutgoingEdge(Vertex v, Vertex other, String type);

	public Future<Iterable<Edge>> addOperationVertexGetOutgoingEdges(Vertex v, String type);

	public Future<Edge> addOperationVertexPutIncomingEdge(Vertex v, Vertex other, String type);

	public Future<Iterable<Edge>> addOperationVertexGetIncomingEdges(Vertex v, String type);

	public Iterable<String> getIterableVertexPropertyKeys(Vertex v);

	public Iterable<byte[]> getIterableVertexPropertyValues(Vertex v);

	public Iterable<Edge> getIterableVertexIncomingEdges(Vertex v, String type);

	public Iterable<Edge> getIterableVertexOutgoingEdges(Vertex v, String type);
}