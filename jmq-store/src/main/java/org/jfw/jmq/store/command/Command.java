package org.jfw.jmq.store.command;

import java.nio.ByteBuffer;

import org.jfw.jmq.store.StoreMeta;
import org.jfw.jmq.store.StoreService;

public interface Command {
	void writeRedo(ByteBuffer buf) ;
	void afterCommit(StoreService storeService);
	boolean readRedo(ByteBuffer in);
	void apply(StoreMeta meta);
	void store(StoreService storeService);
	int getRedoSize();
}
