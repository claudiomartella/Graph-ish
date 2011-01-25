package org.acaro.stagedgraphish.operations;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.Vertex;

public class EdgeFilter {
	private Vertex vertex;
	private Direction direction;
	private String type;
	private Edge last;
	private int size;
	
	public EdgeFilter(Vertex vertex){
		this.vertex = vertex;
	}
	
	public Vertex getVertex(){
		return this.vertex;
	}
	
	public void setDirection(Direction direction){
		this.direction = direction;
	}
	
	public Direction getDirection(){
		return this.direction;
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	public String getType(){
		return this.type;
	}
	
	public void setLast(Edge edge){
		this.last = edge;
	}
	
	public Edge getLast(){
		return last;
	}
	
	public void setSize(int size){
		this.size = size;
	}
	
	public int getSize(){
		return this.size;
	}
}