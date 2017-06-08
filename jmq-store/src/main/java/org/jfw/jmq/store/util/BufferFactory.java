package org.jfw.jmq.store.util;

import java.nio.ByteBuffer;

public class BufferFactory {
	public ByteBuffer getBuffer(){
		return ByteBuffer.allocateDirect(4096);
	}
	public ByteBuffer getTypeBuffer(){
		return ByteBuffer.allocateDirect(8);
	}
	public ByteBuffer getSizeBuffer(){
		return ByteBuffer.allocateDirect(2);
	}
	
	public void freeBuffer(ByteBuffer buf){
		
	}
	public void freeTypeBuffer(ByteBuffer buf){
		
	}
	public void freeSizeBuffer(ByteBuffer buf){
		
	}

}
