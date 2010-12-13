package org.acaro.graphish;

import java.io.IOException;
import java.util.UUID;

public class Graphish  {
	private GraphStore store;
	
	public Graphish(GraphStore store) throws IOException {
		this.store = store;
	}

	protected boolean hasProperty(Vertex vertex, String key) {
		// TODO Auto-generated method stub
		return false;
	}

	protected byte[] setProperty(Vertex vertex, String key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	protected byte[] getProperty(Vertex vertex, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	protected byte[] removeProperty(Vertex vertex, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	protected Iterable<String> getPropertyKeys(Vertex vertex) {
		// TODO Auto-generated method stub
		return null;
	}

	protected Iterable<byte[]> getPropertyValues(Vertex vertex) {
		// TODO Auto-generated method stub
		return null;
	}

	protected boolean hasProperty(Edge edge, String key) {
		// TODO Auto-generated method stub
		return false;
	}
	
	protected byte[] setProperty(Edge edge, String key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	protected byte[] getProperty(Edge edge, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	protected byte[] removeProperty(Edge edge, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	protected Iterable<String> getPropertyKeys(Edge edge) {
		// TODO Auto-generated method stub
		return null;
	}

	protected Iterable<byte[]> getPropertyValues(Edge edge) {
		// TODO Auto-generated method stub
		return null;
	}

	protected Edge createEdge(Vertex from, Vertex to, String type) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Vertex createVertex();
	
}