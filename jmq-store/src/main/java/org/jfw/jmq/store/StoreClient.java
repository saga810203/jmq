package org.jfw.jmq.store;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

public interface StoreClient {
	AsynchronousSocketChannel getChannel();
	long readType();
	short readSize();
	
	

}
