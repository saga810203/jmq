package org.jfw.jmq.store.impl;

import java.nio.channels.AsynchronousFileChannel;

import org.jfw.jmq.store.Topic;

public class DefaultTopic /*extends LoopFile*/ implements Topic {
	private	String name;
	private short index;
	private volatile long p;
	private volatile long l;
	private volatile long c;
	
	public String getName() {
		return name;
	}

	public short getIndex() {
		return index;
	}

	

	public boolean load() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public AsynchronousFileChannel getChannel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void capacity(long capacity) {
		this.c = capacity;		
	}

	@Override
	public synchronized void limit(long limit) {
		this.l = limit;
	}

	@Override
	public void position(long position) {
		this.p = position;
	}

	@Override
	public synchronized long allocate(long size) {
		long ret = -1, np = p+size;
		if(p< l){
			if(np<l){
				ret = p;
				p = np;
			}
		}else if(p>l){
			if(np<c){
				ret = p ;
				p = np;
			}else if(size< l){
            	 ret = 0;
            	 p = size;
             }			
		}
		return ret;// include p==l
	}




}
