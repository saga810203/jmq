package org.jfw.jmq.store.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.zip.Adler32;

public class OutputFileSegment  extends OutputStream{
	
	private ArrayList<ByteBuffer> buffers;
	private BufferFactory bf = null;
	private int length = 0;
	private ByteBuffer current;
	private ByteBuffer head;
	
	
	
	public void reset(BufferFactory bf){
		if(this.bf!=null){
			this.freeBuffer();
		}
		if(this.buffers == null) {
			this.buffers = new ArrayList<ByteBuffer>();
		}
		this.bf = bf;
		this.length = 0;
		this.head = this.bf.getBuffer();
		this.current = head;
		this.head.position(12);
		this.buffers.add(this.head);
	}
	
	
	private void freeBuffer(){
		if(this.buffers!=null && (!this.buffers.isEmpty())){
			for(ByteBuffer b:this.buffers){
				this.bf.freeBuffer(b);
			}
		}
		this.buffers.clear();
	}
	
	
	

	@Override
	public void write(int b){
		if(this.current.hasRemaining()){
			this.current.put((byte)b);
		}else{
			this.current.flip();
			this.current = this.bf.getBuffer();
			this.buffers.add(this.current);
			this.current.put((byte)b);
		}
		++this.length;
	}

	@Override
	public void write(byte[] b) {
		this.write(b,0,b.length);
	}

	@Override
	public void write(byte[] b, int off, int len)  {
		while(len>0){
			int l = this.current.remaining();
			if(l>0){
				if(l>=len){
					this.current.put(b,off, len);
					this.length=+len;
				}else{
					this.current.put(b,off,l);
					off=+l;
					len-=l;
					this.length+=l;
				}
			}else{
				this.current.flip();
				this.current = this.bf.getBuffer();
				this.buffers.add(current);
			}
		}
		
	}
	public int getLength(){
		return this.length;
	}
	
	protected void checksum(Adler32 adler ){
		if(this.current.position()!=0){
			this.current.flip();
		}
		this.head.position(12);
		adler.reset();
		for(ByteBuffer b:this.buffers){
			adler.update(b);
			b.rewind();
		}
		this.head.putInt(this.length+8);
		this.head.putLong(adler.getValue());
		this.head.rewind();
		
	}
	
	public void flushToFile(AsynchronousFileChannel channel,Adler32 adler,long position, boolean forceMeta) throws InterruptedException, ExecutionException, IOException{
		this.checksum(adler);
		IOHelper.store(channel, buffers, position);
		channel.force(forceMeta);		
	}
	
}
