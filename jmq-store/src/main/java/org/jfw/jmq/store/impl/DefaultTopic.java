package org.jfw.jmq.store.impl;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;

import org.jfw.jmq.store.MessageIndex;
import org.jfw.jmq.store.ReadListener;
import org.jfw.jmq.store.Topic;
import org.jfw.jmq.store.WriteListener;
import org.jfw.jmq.store.exception.StoreException;

public class DefaultTopic extends LoopFile implements Topic {
	private	String name;
	private short index;
	
	public String getName() {
		return name;
	}

	public short getIndex() {
		return index;
	}

	public void write(AsynchronousSocketChannel channel, WriteListener wl) {
		
	}

	public void read(long index, short size, AsynchronousSocketChannel channel, ReadListener rl) {
		// TODO Auto-generated method stub
		
	}

	public boolean load(boolean aborted) {
		// TODO Auto-generated method stub
		return false;
	}

	public void writeIndex(List<MessageIndex> idxs) throws StoreException {
		// TODO Auto-generated method stub
		
	}

	public List<MessageIndex> loadIndex(long start, long end) throws StoreException {
		// TODO Auto-generated method stub
		return null;
	}

}
