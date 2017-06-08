package org.jfw.jmq.store.impl;

public class LoopFile {
	private long c;
	/**
	 *  0 =< l < c
	 */
	private long l;
	private long p;
	
	
	public long getCapacity() {
		return c;
	}
	public void setCapacity(long capacity) {
		this.c = capacity;
	}
	public long getLimit() {
		return l;
	}
	public void setLimit(long limit) {
		this.l = limit;
	}
	public long getPosition() {
		return p;
	}
	public void setPosition(long position) {
		this.p = position;
	}
	
	synchronized public long allocate(long size){
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
	synchronized public void limit(long val){
		this.l = val;
	}
}
