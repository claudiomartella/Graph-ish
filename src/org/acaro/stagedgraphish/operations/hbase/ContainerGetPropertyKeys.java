package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ContainerGetPropertyKeys implements Callable<List<String>> {
	private byte[] family;
	private byte[] id;
	
	public ContainerGetPropertyKeys(byte[] family, byte[] id){
		this.family = family;
		this.id     = id;
	}
	
	public List<String> call() throws Exception {
		HTableInterface table = HBaseStore.getTable();
		List<String> keys = new LinkedList<String>();
		
		try {
			Get g = new Get(id);
			Result res = table.get(g);
			if(!res.isEmpty()){
				for(byte[] key: res.getFamilyMap(family).keySet()){
					keys.add(Bytes.toString(key));
				}
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return keys;
	}
}