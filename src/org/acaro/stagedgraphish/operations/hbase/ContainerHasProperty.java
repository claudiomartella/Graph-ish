package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class ContainerHasProperty implements Callable<Boolean> {
	private byte[] family;
	private byte[] id;
	private String key;
	
	public ContainerHasProperty(byte[] family, byte[] id, String key){
		this.family = family;
		this.id     = id;
		this.key    = key;
	}
	
	public Boolean call() throws Exception {
		boolean ret;
		HTableInterface table = HBaseStore.getTable();
		
		try {
			Get g = new Get(id);
			g.addColumn(family, Bytes.toBytes(key));
			ret = table.exists(g);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return ret;
	}
}
