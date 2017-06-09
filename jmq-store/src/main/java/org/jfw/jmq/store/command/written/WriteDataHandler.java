package org.jfw.jmq.store.command.written;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public final class WriteDataHandler implements CompletionHandler<Integer, WriteCommand>{
	
	public static final  WriteDataHandler INS = new WriteDataHandler();
	
	private WriteDataHandler() {
	}

	@Override
	public void completed(Integer result, WriteCommand w) {
		ByteBuffer buf = w.getBuf();
		if(result>0){
			w.incWritoePos(result);
			if(!buf.hasRemaining()){
				if(w.isChunked()){
					w.readSize();
				}else{
					w.done(w.getWritoePos()-1);
				}
			}
		}
		w.getWriter().write(w.getBuf(),w.getWritoePos(),w,this);
	}

	@Override
	public void failed(Throwable exc, WriteCommand w) {
		w.failWithWirte(exc);		
	}

}
