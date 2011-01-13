package org.acaro.stagedgraphish;

import java.util.UUID;

public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//byte[] from = Bytes.fromUuid(UUID.randomUUID()).toByteArray();
		byte[] from = Bytes.fromUTF8("prima").toByteArray();
		//byte[] to = Bytes.fromUuid(UUID.randomUUID()).toByteArray();
		byte[] to = Bytes.fromUTF8("seconda").toByteArray();
		String type = "knows";
		byte[] typeb = Bytes.fromUTF8(type).toByteArray();
		byte[] EDGE_CONJ = { 0x2b };
		
		byte[] buff = new byte[from.length+to.length+1+type.length()+1];
		System.arraycopy(from, 0, buff, 0, from.length);
		System.arraycopy(EDGE_CONJ, 0, buff, from.length, 1);
		System.arraycopy(to, 0, buff, from.length+1, to.length);
		System.arraycopy(EDGE_CONJ, 0, buff, from.length+to.length+1, 1);
		System.arraycopy(typeb, 0, buff, from.length+to.length+2, typeb.length);
		
		System.out.println(Bytes.toUTF8(buff));
	}

}
