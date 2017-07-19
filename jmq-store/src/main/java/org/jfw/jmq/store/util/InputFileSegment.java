package org.jfw.jmq.store.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.zip.Adler32;

public class InputFileSegment extends InputStream {
	private int remaining;
	private long pos;
	private AsynchronousFileChannel channel;
	private ArrayList<ByteBuffer> buffers;
	private BufferFactory bf;
	private int idx ;
	
	
	public void reset(BufferFactory bf,AsynchronousFileChannel channel,long pos){
		this.freeBuffer();
		this.bf = bf;
		this.channel = channel;
		this.pos = pos;
		this.buffers = null;
		this.remaining = 0;
		this.idx=Integer.MIN_VALUE;
	}
	private int load(ByteBuffer buf,long position,int size) throws InterruptedException, ExecutionException{
		int ret = size;
		while(size>0 && buf.hasRemaining()){
			int rlen = this.channel.read(buf, position).get();
			if(rlen>0){
				position+=rlen;
				size-=rlen;
			}
		}
		if(size<0){
			buf.limit(buf.limit()+size);
		}
		buf.flip();
		return ret;		
	}
	
	
	/**
	 * 加载
	 * @param adler
	 * @return -1:error file segment(file length is little) -2 error  filesegment length invalid  -3   checksum error      >0 filesegment data legth 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public int load(Adler32 adler) throws InterruptedException, ExecutionException{
		long checksum = 0;
		ByteBuffer buf = this.bf.getBuffer();
		buf.limit(4);
		int rlen = this.load(buf,pos, 4);
		if(rlen!=4){
			return -1;
		}
		rlen = buf.getInt();
		if(rlen<=8){
			return -2;
		}
		int rSize = rlen;
		this.remaining = rSize -8;
		long np = this.pos+4;
		buf.clear();
		this.buffers = new ArrayList<ByteBuffer>();
		this.buffers.add(buf);
		while(rSize>0){
			rlen = this.load(buf,np, rSize);
			if(rlen==0){
				return -1;
			}else if(rlen==rSize){
				if(this.buffers.size()==1){
					checksum = buf.getLong();
					adler.reset();
				}
				buf.mark();
				adler.update(buf);
				buf.reset();					
				break;
			}else if(buf.capacity() == buf.limit()){
				rSize-=rlen;
				np+=rlen;
				if(this.buffers.size()==1){
					checksum = buf.getLong();
					adler.reset();
				}
				buf.mark();
				adler.update(buf);
				buf.reset();				
				buf = this.bf.getBuffer();
				this.buffers.add(buf);
			}
		}
		if(checksum != adler.getValue()){
			return -3;
		}
		this.idx = 0;
		return this.remaining;				
	}
	
	public void freeBuffer(){
		if(this.buffers!=null && (!this.buffers.isEmpty())){
			for(ByteBuffer b:this.buffers){
				this.bf.freeBuffer(b);
			}
		}
		this.buffers = null;
	}
	
	
	
	@Override
	public int read(byte[] b) throws IOException {
		return this.read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if(this.remaining >0){			
			if(this.remaining < len){
				len =(int) this.remaining;
			}
			int ret = len;
			while(len>0){
				int num =this.buffers.get(idx).remaining();
				if(num>0){
					if(num>=len){
						this.buffers.get(idx).get(b, off, len);
						break;
					}else{
						this.buffers.get(idx).get(b, off, num);
						len-=num;
						off+=num;
						++idx;
					}	
				}else{
					++idx;
				}
			}
			this.remaining-=ret;
			return ret;
			
		}
		return -1;
	}

	@Override
	public long skip(long n) throws IOException {
			if(n<1 || 0== this.remaining) return 0;
			if(n>this.remaining || n== this.remaining){
				n = this.remaining;
				this.remaining =0;
				return n;
			}
			long ret = n;
			while(n>0){
				int len =this.buffers.get(idx).remaining();
				if(len > n ){
					this.buffers.get(idx).position((int) (this.buffers.get(idx).position()+n));
					break;
				}else if(len ==n){
					++idx;
				}else{
					n-=len;
					++idx;
				}
			}
			this.remaining-=ret;
			return ret;
			
	}

	@Override
	public int available() throws IOException {
		return  this.remaining;
	}

	@Override
	public void close() throws IOException {
		this.freeBuffer();
	}

	@Override
	public int read() throws IOException {
		if (this.remaining > 0) {
			while (!this.buffers.get(idx).hasRemaining()) {
				++idx;
			}
			byte b = this.buffers.get(idx).get();
			--this.remaining;
			return ((int) b) & 0xFF;

		}
		return -1;
	}
}
