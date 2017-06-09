package org.jfw.jmq.store;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

import org.jfw.jmq.store.command.Command;

public interface StoreClient {
	AsynchronousSocketChannel getChannel();
	ByteBuffer allocateBuf(); 
	void free(ByteBuffer buf);
	Topic getCurrentTopic();
	void over(Command cmd);
}
