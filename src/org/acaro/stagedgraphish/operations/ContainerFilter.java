package org.acaro.stagedgraphish.operations;

import org.acaro.stagedgraphish.PropertyContainer;

public class ContainerFilter {
	private PropertyContainer last;
	private int size;

	public void setSize(int size){
		this.size = size;
	}
	
	public int getSize(){
		return size;
	}
	
	public void setLast(PropertyContainer container){
		this.last = container;
	}
	
	public PropertyContainer getLast(){
		return this.last;
	}
}
