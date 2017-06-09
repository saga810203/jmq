package org.jfw.jmq.store.command;

import org.jfw.jmq.store.StoreClient;
import org.jfw.jmq.store.Topic;

public interface Command {
	void exec(StoreClient client);
	void commit(StoreClient client);
	
}
