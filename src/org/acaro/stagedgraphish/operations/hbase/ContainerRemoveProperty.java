package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ContainerRemoveProperty implements Callable<byte[]> {
	private byte[] family;
	private byte[] id;
	private String key;
	
	public ContainerRemoveProperty(byte[] family, byte[] id, String key){
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
			Delete d = new Delete(id);
			d.deleteColumn(family, property);
			table.delete(d);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.getInstance().putTable(table);
		}
		
		return ret;
	}
}
