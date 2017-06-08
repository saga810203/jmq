package org.jfw.jmq.store.impl;

import java.nio.channels.AsynchronousSocketChannel;

import org.jfw.jmq.store.MessageIndex;
import org.jfw.jmq.store.WriteListener;

public class DefaultWriteListener  implements WriteListener{
	private AsynchronousSocketChannel chnanel;
	
	public AsynchronousSocketChannel getChnanel() {
		return chnanel;
	}

	public void setChnanel(AsynchronousSocketChannel chnanel) {
		this.chnanel = chnanel;
	}

	public void done(MessageIndex index) {
		// TODO Auto-generated method stub
		
	}

	public void fail(int error, Object msg) {
		// TODO Auto-generated method stub
		
	}

}
