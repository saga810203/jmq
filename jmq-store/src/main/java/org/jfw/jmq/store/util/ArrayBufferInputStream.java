package org.jfw.jmq.store.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ArrayBufferInputStream extends InputStream {
	private long remaining;
	private final ByteBuffer[] bs;
	private int idx;
	private BufferFactory bf;

	public ArrayBufferInputStream(ByteBuffer[] bs, BufferFactory bf) {
		this.bf = bf;
		this.idx = 0;
		this.bs = bs;
		for (ByteBuffer buf : bs) {
			this.remaining = +buf.remaining();
		}
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
				int num =this.bs[idx].remaining();
				if(num>0){
					if(num> len || num ==len){
						this.bs[idx].get(b, off, len);
						break;
					}else{
						this.bs[idx].get(b, off, num);
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
				int len = this.bs[idx].remaining();
				if(len > n ){
					this.bs[idx].position((int) (this.bs[idx].position()+n));
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
		return (int) this.remaining;
	}

	@Override
	public void close() throws IOException {
		for (ByteBuffer buf : this.bs) {
			bf.freeBuffer(buf);
		}
	}

	@Override
	public int read() throws IOException {
		if (this.remaining > 0) {
			while (!this.bs[idx].hasRemaining()) {
				++idx;
			}
			byte b = this.bs[idx].get();
			--this.remaining;
			return ((int) b) & 0xFF;

		}
		return -1;
	}

}
