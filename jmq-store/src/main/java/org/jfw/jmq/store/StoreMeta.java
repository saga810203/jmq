package org.jfw.jmq.store;

import java.util.HashMap;
import java.util.Map;

public class StoreMeta {

	public void copy(StoreMeta meta) {

	}

	private long time;
	private Map<String, TopicMeta> topics = new HashMap<String, TopicMeta>();
	private int capacity;
	private int position;
	private long redoTime;
	private boolean clean;

	public StoreMeta() {
		this.time = 0;
		this.capacity = 1024 * 1024 * 128;
		this.position = 0;
		this.clean = true;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public Map<String, TopicMeta> getTopics() {
		return topics;
	}

	public void setTopics(Map<String, TopicMeta> topics) {
		this.topics = topics;
	}

	public int getCapacity() {
		return capacity;
	}

	public void setCapacity(int capacity) {
		this.capacity = capacity;
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public boolean isClean() {
		return clean;
	}

	public void setClean(boolean clean) {
		this.clean = clean;
	}

	public long incTime() {
		++this.time;
		return this.time;
	}
	public void decTime(){
		--this.time;
	}

	public long getRedoTime() {
		return redoTime;
	}

	public void setRedoTime(long redoTime) {
		this.redoTime = redoTime;
	}

}
