package org.jfw.jmq.store.command;

import org.jfw.jmq.store.StoreClient;

public interface Command {
	void exec(StoreClient client);
}
