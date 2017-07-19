package org.jfw.jmq.store.util;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Adler32;

public class OutputFileSegment  extends OutputStream{
	
	private ArrayList<ByteBuffer> buffers;
	private boolean opened;
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
		this.opened = true;
		this.length = 0;
		this.current = null;
		this.head = null;
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
		this.checkOpened();
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
		this.checkOpened();
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



	@Override
	public void close() {
		if(this.opened){
			this.current.flip();
		}
		this.opened = false;
	}

	
	private void checkOpened(){
		if(!this.opened) throw new IllegalStateException("OutputFileSegment is already closed");
		if(this.head== null){
			this.head = this.bf.getBuffer();
			this.current = head;
			this.head.position(12);
			this.buffers.add(this.head);
		}
	}
	
	public int getLength(){
		return this.length;
	}
	
	public List<ByteBuffer> getBuffers(Adler32 adler ){
		if(this.opened) throw new IllegalStateException("OutputFileSegment is opened");
		this.head.position(12);
		adler.reset();
		for(ByteBuffer b:this.buffers){
			adler.update(b);
			b.rewind();
		}
		this.head.putInt(this.length);
		this.head.putLong(adler.getValue());
		this.head.rewind();
		return this.buffers;		
	}
	
	
	

}
