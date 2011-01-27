package org.acaro.stagedgraphish;

import java.util.concurrent.Future;

public interface Edge extends PropertyContainer {
	
	public Vertex getFrom();
	
	public Vertex getTo();
	
	public Vertex getOther(Vertex vertex);
	
	public String getType();
	
	public Future<Void> delete();
}