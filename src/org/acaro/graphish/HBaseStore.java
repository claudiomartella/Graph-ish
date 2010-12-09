package org.acaro.graphish;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseStore implements GraphStore {
	private HTable htable;
	private boolean readonly;
	
	public HBaseStore(String db, boolean readonly) throws IOException{
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, db);
		this.readonly = readonly;
	}

	public void deleteVertex(OldVertex v) {
		// TODO Auto-generated method stub
		
	}

	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	public Edge retrieveEdge(long id) {
		// TODO Auto-generated method stub
		return null;
	}

	public OldVertex retrieveVertex(long id) {
		// TODO Auto-generated method stub
		return null;
	}
}