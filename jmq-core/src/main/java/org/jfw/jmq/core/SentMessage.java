package org.jfw.jmq.core;

import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.CompletionHandler;

public interface SentMessage {
	
	
	SentMessage type(long type);
	void send(AsynchronousByteChannel channel,CompletionHandler<Integer, SentMessage> handler );
}
