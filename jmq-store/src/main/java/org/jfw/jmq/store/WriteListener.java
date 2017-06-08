package org.jfw.jmq.store;

import java.nio.ByteBuffer;

public interface WriteListener {
	/**
	 * write message content success;
	 */
	void done(MessageIndex index);
	/**
	 * write message content fail
	 * @param error  :code  0: unknow     1 :  topic is un recover   2 : topic is readonly  3 :  write error  4 : force flush error 5:create file error 6:open file error
	 */
	void fail(int error,Object msg);

}
