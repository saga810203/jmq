package org.jfw.jmq.store;

public interface ReadListener {
	/**
	 * read message content success;
	 */
	void done();
	/**
	 * read message content fail
	 * @param error  :code  0: unknow     1 :  topic is un recover   2 : not exists   3:open file error 4:read error
	 */
	void fail(int error,Object msg);
}
