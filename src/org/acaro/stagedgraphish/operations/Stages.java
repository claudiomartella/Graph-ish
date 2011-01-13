package org.acaro.stagedgraphish.operations;

import org.acaro.stagedgraphish.OldHBaseStore;
import org.acaro.stagedgraphish.operations.hbase.HBaseStore;

public class Stages {
	static StorageStage store;
	
	static {
		store = new HBaseStore();
	}
	
	public static StorageStage getStore(){
		return store;
	}
}
