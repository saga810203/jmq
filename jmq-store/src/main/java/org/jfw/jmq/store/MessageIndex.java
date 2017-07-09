package org.jfw.jmq.store;

public class MessageIndex {
	/*
	 * 
	 */
	private long id;
	/*
	 * postion in topic data store
	 */
	private long position;
	/**
	 * message type
	 */
	private long type;
	private short size;
	private long limit;
	private long time;
	private long nextPosition;
	
	
	
	
	public long getNextPosition() {
		return nextPosition;
	}
	public void setNextPosition(long nextPosition) {
		this.nextPosition = nextPosition;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public long getLimit() {
		return limit;
	}
	public void setLimit(long limit) {
		this.limit = limit;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
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
