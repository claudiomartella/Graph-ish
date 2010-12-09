package org.acaro.graphish;

import java.util.UUID;

public interface Edge extends PropertyContainer {
	
	public byte[] getId();
	
	public Vertex getFrom();
	
	public Vertex getTo();
	
	public Vertex getOther(Vertex vertex);
	
	public String getType();
}