package org.jfw.jmq.store;

public class MessageIndex {
	/*
	 * 
	 */
	private long id;
	/*
	 * postion in topic data store
	 */
	private long postion;
	/**
	 * message type
	 */
	private long type;
	private short size;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getPostion() {
		return postion;
	}
	public void setPostion(long postion) {
		this.postion = postion;
	}
	public long getType() {
		return type;
	}
	public void setType(long type) {
		this.type = type;
	}
	public short getSize() {
		return size;
	}
	public void setSize(short size) {
		this.size = size;
	}
}
