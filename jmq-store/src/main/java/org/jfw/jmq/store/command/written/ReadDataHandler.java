package org.jfw.jmq.store.command.written;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public final class ReadDataHandler implements CompletionHandler<Integer, WriteCommand>{
	
	public static final  ReadDataHandler INS = new ReadDataHandler();
	
	private ReadDataHandler() {
	}

	@Override
	public void completed(Integer result, WriteCommand w) {
		ByteBuffer buf = w.getBuf();
		if(result>0){
			if(!buf.hasRemaining()){
				w.writeData();
				return;
			}
		}
		w.getReader().read(buf,w,this);
	}

	@Override
	public void failed(Throwable exc, WriteCommand w) {
		w.failWithRead(exc);		
	}

}
