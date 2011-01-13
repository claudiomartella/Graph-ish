package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.DoesntExist;
import org.acaro.stagedgraphish.StorageException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ContainerSetProperty implements Callable<Void> {
	private byte[] family;
	private byte[] id;
	private String key;
	private byte[] value;
	
	public ContainerSetProperty(byte[] family, byte[] id, String key, byte[] value){
		this.family = family;
		this.id     = id;
		this.key    = key;
		this.value  = value;
	}
	
	public Void call() throws Exception {
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = HBaseStore.getTable();
		
		try {
			if(!HBaseStore.recordExists(table, id)){
				throw new DoesntExist(id);
			}
			Put p = new Put(id);
			p.add(family, property, value);
			table.put(p);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return null;
	}
}
