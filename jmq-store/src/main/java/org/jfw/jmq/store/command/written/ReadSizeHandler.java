package org.jfw.jmq.store.command.written;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public final class ReadSizeHandler implements CompletionHandler<Integer, WriteCommand>{
	
	public static final  ReadSizeHandler INS = new ReadSizeHandler();
	
	private ReadSizeHandler() {
	}

	@Override
	public void completed(Integer result, WriteCommand w) {
		ByteBuffer buf = w.getBuf();
		if(result>0){
			if(!buf.hasRemaining()){
				buf.flip();
				w.setSize(buf.getShort());
				w.read();
				return;
			}
		}
		w.getReader().read(buf,w,this);
	}

	@Override
	public void failed(Throwable exc, WriteCommand w) {
		w.failWithReadSize(exc);		
	}

}
