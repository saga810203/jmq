package org.jfw.jmq.store;

public interface Message {
	void hold();
	void replace();
}
