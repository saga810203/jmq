package org.jfw.jmq.store.command;

import java.io.InputStream;
import java.io.OutputStream;

import org.jfw.jmq.store.StoreClient;
import org.jfw.jmq.store.StoreMeta;
import org.jfw.jmq.store.StoreService;

public interface Command {
	void writeRedo(OutputStream out) ;
	void afterCommit(StoreService storeService,StoreClient storeClient);
	void readRedo(InputStream in);
	void afterCheckPoint(StoreMeta meta);
}
