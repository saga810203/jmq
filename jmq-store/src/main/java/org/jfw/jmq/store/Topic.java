package org.jfw.jmq.store;

import java.nio.channels.AsynchronousFileChannel;

public interface Topic {
	String getName();
	short getIndex();
	AsynchronousFileChannel getChannel();
	void capacity(long capacity);
	void limit(long limit);
	void position(long position);
	long allocate(long size);
	boolean load();	
}
