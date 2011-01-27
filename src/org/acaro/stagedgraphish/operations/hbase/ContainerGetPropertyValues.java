package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

public class ContainerGetPropertyValues implements Callable<List<byte[]>> {
	private byte[] family;
	private byte[] id;
	
	public ContainerGetPropertyValues(byte[] family, byte[] id){
		this.family = family;
		this.id     = id;
	}
	
	public List<byte[]> call() throws Exception {
		HTableInterface table = HBaseStore.getInstance().getTable();
		List<byte[]> values = new ArrayList<byte[]>();
		
		try {
			Get g = new Get(id);
			Result res = table.get(g);
			if(!res.isEmpty()){
				values.addAll(res.getFamilyMap(family).values());
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.getInstance().putTable(table);
		}
		
		return values;
	}
}