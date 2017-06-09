package org.jfw.jmq.store.command.written;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public final class WritePosHandler implements CompletionHandler<Integer, WriteCommand>{
	
	public static final  WritePosHandler INS = new WritePosHandler();
	
	private WritePosHandler() {
	}

	@Override
	public void completed(Integer result, WriteCommand w) {
		ByteBuffer buf = w.getBuf();
		if(result>0){
			if(!buf.hasRemaining()){
				w.setWritoePos(-1);
				w.readData();
				return;
			}else{
				w.incWritoePos(result);
			}
		}
		w.getWriter().write(buf, w.getWritoePos(),w,this);
	}

	@Override
	public void failed(Throwable exc, WriteCommand w) {
		w.failWithWirte(exc);		
	}

}
