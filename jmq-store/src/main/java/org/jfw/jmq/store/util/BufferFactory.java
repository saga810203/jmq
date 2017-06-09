package org.jfw.jmq.store.util;

import java.nio.ByteBuffer;

public class BufferFactory {
	public ByteBuffer getBuffer(){
		return ByteBuffer.allocateDirect(8192);
	}
	public void freeBuffer(ByteBuffer buf){
		
	}
}
