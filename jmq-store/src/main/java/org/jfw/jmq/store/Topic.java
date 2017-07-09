package org.jfw.jmq.store;

import java.nio.channels.AsynchronousFileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.jfw.jmq.store.config.TopicConfig;
import org.jfw.jmq.store.exception.StoreException;

public class Topic {

	public Topic(String name, AsynchronousFileChannel channel, long position, long limit, long capacity,
			TopicConfig config) {
		this.name = name;
		this.channel = channel;
		this.position = position;
		this.limit = limit;
		this.capacity = capacity;
		this.config = config;
	}

	public String getName() {
		return this.name;
	}

	public AsynchronousFileChannel getChannel() {
		return this.channel;
	}

	private long allocate(long size) {
		long nextPosition = position + size;
		long ret = position;
		if (position > limit) {
			if (nextPosition < capacity) {
				position = nextPosition;
			} else if (nextPosition == capacity) {
				position = 0;
			} else if (size < limit) {
				position = size;
				ret = 0;
			}
		} else if (position < limit) {
			if (nextPosition < limit || nextPosition == limit) {
				position = nextPosition;
			} else {
				ret = -1;
			}
		} else {
			ret = -1;
		}
		return ret;
	}

	public MessageIndex build(long size) {
		lock.lock();
		try {
			long p = this.allocate(size);
			if (p > -1) {
				MessageIndex mi = new MessageIndex();
				mi.setPosition(p);
				mi.setLimit(p);
				mi.setTime(System.currentTimeMillis());
				mi.setNextPosition(this.position);
				this.tempMsgList.add(mi);
				return mi;
			}
			return null;
		} finally {
			lock.unlock();
		}
	}

	public long allocateNextChunk(long size, MessageIndex mi) {
		lock.lock();
		try {
			long ret = this.allocate(size);
			if(ret>-1){
				mi.setNextPosition(this.position);
			}
			return ret;
		} finally {
			lock.unlock();
		}
	}

	public void removeMessageIndex(List<MessageIndex> mis) {
		this.tempMsgList.removeAll(mis);
	}

	private LinkedList<MessageIndex> tempList = new LinkedList<MessageIndex>();

	public void buildLimt(List<MessageIndex> mis) throws StoreException {
		lock.lock();
		try {
			long currentTime = System.currentTimeMillis();
			tempList.clear();
			tempList.addAll(this.tempMsgList);
			tempList.removeAll(mis);
			for (MessageIndex mi : mis) {
				if (currentTime - mi.getTime() > this.config.getMaxSurvivalTime()) {
					throw new StoreException(StoreException.TEMP_MESSAGE_TIMEOUT, this.name);
				}
				long mlimit = mi.getPosition();
				for (MessageIndex ami : tempList) {
					if (currentTime - ami.getTime() <= this.config.getMaxSurvivalTime()) {
						long alimit = ami.getPosition();
						if (mlimit > this.limit) {
							if (alimit > this.limit && alimit < mlimit) {
								mlimit = alimit;
							}
						} else {
							if ((alimit < mlimit) || (alimit > mlimit && alimit > this.limit)) {
								mlimit = alimit;
							}
						}
					}
				}
				mi.setLimit(mlimit);
			}
		} finally {
			lock.unlock();
		}
	}

	public boolean capacity(long capacity) {
		lock.lock();
		try {
			if (capacity > this.capacity) {
				this.capacity = capacity;
				return true;
			} else if (capacity < this.capacity) {
				if (position > limit && position < capacity) {
					this.capacity = capacity;
					return true;
				}
			}
		} finally {
			lock.unlock();
		}
		return false;
	}
	public void limit(long limit){
		lock.lock();
		try{
		this.limit = limit;
		}finally{
			lock.unlock();
		}
	}

	private String name;
	private AsynchronousFileChannel channel;
	private LinkedList<MessageIndex> tempMsgList = new LinkedList<MessageIndex>();
	private long position = 0;
	private long limit;
	private long capacity;
	private TopicConfig config;
	private ReentrantLock lock = new ReentrantLock();

}
