package org.acaro.graphish;

import org.acaro.stagedgraphish.PropertyContainer;

public interface Edge extends PropertyContainer {
	
	public Vertex getFrom();
	
	public Vertex getTo();
	
	public Vertex getOther(Vertex vertex);
	
	public String getType();
}