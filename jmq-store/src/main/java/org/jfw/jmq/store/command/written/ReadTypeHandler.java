package org.jfw.jmq.store.command.written;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public final class ReadTypeHandler implements CompletionHandler<Integer, WriteCommand>{
	
	public static final  ReadTypeHandler INS = new ReadTypeHandler();
	
	private ReadTypeHandler() {
	}

	@Override
	public void completed(Integer result, WriteCommand w) {
		ByteBuffer buf = w.getBuf();
		if(result>0){
			if(!buf.hasRemaining()){
				buf.flip();
				w.setType(buf.getLong());
				w.readSize();
				return;
			}
		}
		w.getReader().read(buf,w,this);
	}

	@Override
	public void failed(Throwable exc, WriteCommand w) {
		w.failWithReadType(exc);		
	}

}
