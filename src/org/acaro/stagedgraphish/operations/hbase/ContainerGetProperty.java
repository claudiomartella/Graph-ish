package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ContainerGetProperty implements Callable<byte[]> {
	private byte[] family;
	private byte[] id;
	private String key;
	
	public ContainerGetProperty(byte[] family, byte[] id, String key){
		this.family = family;
		this.id     = id;
		this.key    = key;
	}
	
	public byte[] call() throws Exception {
		byte[] ret = null;
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = HBaseStore.getInstance().getTable();
		
		try {
			Get g = new Get(id);
			g.addColumn(family, property);
			Result res = table.get(g);
			if(!res.isEmpty()){
				ret = res.getValue(family, property);
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.getInstance().putTable(table);
		}
		
		return ret;
	}
}