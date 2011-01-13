package org.acaro.stagedgraphish.operations;

import org.acaro.stagedgraphish.Direction;

public class EdgeFilter {
	private Direction direction;
	private String type;
	
	public EdgeFilter(Direction direction, String type){
		this.direction = direction;
		this.type = type;
	}
	
	public Direction getDirection(){ 
		return direction; 
	}
	
	public String getType(){
		return type;
	}
}
