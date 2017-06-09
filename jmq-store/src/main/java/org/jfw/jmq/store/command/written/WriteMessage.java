package org.jfw.jmq.store.command.written;

import org.jfw.jmq.store.Message;
import org.jfw.jmq.store.Topic;

public class WriteMessage implements Message {
	private long id;
	private Topic topic;
	private long type;
	private long position;
	private long end;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public Topic getTopic() {
		return topic;
	}
	public void setTopic(Topic topic) {
		this.topic = topic;
	}
	public long getType() {
		return type;
	}
	public void setType(long type) {
		this.type = type;
	}
	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}

	@Override
	public void hold() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void release() {
		// TODO Auto-generated method stub
		
	}

}
