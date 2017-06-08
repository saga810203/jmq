package org.jfw.jmq.store;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;

import org.jfw.jmq.store.exception.StoreException;

public interface Topic {
	String getName();
	short getIndex();
	void write(AsynchronousSocketChannel channel,WriteListener wl);
	void read(long index,short size, AsynchronousSocketChannel channel,ReadListener rl);
	boolean load(boolean aborted);
	void writeIndex(List<MessageIndex> idxs) throws StoreException;
	List<MessageIndex> loadIndex(long start,long end)throws StoreException;
}
