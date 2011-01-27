package org.acaro.stagedgraphish.operations;

import org.acaro.stagedgraphish.operations.hbase.HBaseStore;

public final class Stages {
	
	private Stages() {}
	
	public static StorageStage getStore(){
		return HBaseStore.getInstance();
	}
}
