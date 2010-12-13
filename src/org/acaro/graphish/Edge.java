package org.acaro.graphish;

public interface Edge extends PropertyContainer {
	
	public Vertex getFrom();
	
	public Vertex getTo();
	
	public Vertex getOther(Vertex vertex);
	
	public String getType();
}