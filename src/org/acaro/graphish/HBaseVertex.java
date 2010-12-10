package org.acaro.graphish;

public class HBaseVertex implements Vertex {

	public boolean hasProperty(String key) {
		return false;
	}

	public byte[] setProperty(String key, byte[] value) {
		GraphStore store = Graphish.getDB();
		return null;
	}

	public byte[] getProperty(String key) {
		return null;
	}

	public byte[] removeProperty(String key) {
		return null;
	}

	public Iterable<String> getPropertyKeys() {
		return null;
	}

	public Iterable<byte[]> getPropertyValues() {
		return null;
	}

	public byte[] getId() {
		return null;
	}

	public Edge putOutgoingEdge(Vertex other, String type) {
		return null;
	}

	public Iterable<Edge> getOutgoingEdges() {
		return null;
	}

	public Iterable<Edge> getOutgoingEdges(String type) {
		return null;
	}

	public Edge putIncomingEdge(Vertex other, String type) {
		return null;
	}

	public Iterable<Edge> getIncomingEdges() {
		return null;
	}

	public Iterable<Edge> getIncomingEdges(String type) {
		return null;
	}
}