package org.jfw.jmq.store.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.Adler32;

public final class IOHelper {
	private IOHelper() {
	}

	public static void mkdirs(File dir) throws IOException {
		if (dir.exists()) {
			if (!dir.isDirectory()) {
				throw new IOException(
						"Failed to create directory '" + dir + "', regular file already existed with that name");
			}

		} else {
			if (!dir.mkdirs()) {
				throw new IOException("Failed to create directory '" + dir + "'");
			}
		}
	}

	public static void close(Closeable csa) {
		if (null != csa) {
			try {
				csa.close();
			} catch (IOException e) {
			}
		}
	}

	public static long readLongSize(AsynchronousFileChannel channel, long position, ByteBuffer buf) throws Exception {
		int tlen = 0;
		while (true) {
			int len = channel.read(buf, position + tlen).get();
			if (len == -1) {
				return -1;
			}
			tlen += len;
			if (tlen >= 8) {
				buf.flip();
				return buf.getLong();
			}
		}
	}

	public static int readIntegerSize(AsynchronousFileChannel channel, long position, ByteBuffer buf) throws Exception {
		int tlen = 0;
		while (true) {
			int len = channel.read(buf, position + tlen).get();
			if (len == -1) {
				return -1;
			}
			tlen += len;
			if (tlen >= 4) {
				buf.flip();
				return buf.getInt();
			}
		}
	}

	public static List<ByteBuffer> readContent(AsynchronousFileChannel channel, long position, long size, BufferFactory bf)
			throws Exception {
		List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
		ByteBuffer buf = bf.getBuffer();
		ret.add(buf);
		long tlen = 0;
		while (true) {
			int len = channel.read(buf, position + tlen).get();
			if (len == -1) {
				for (ByteBuffer b : ret) {
					bf.freeBuffer(b);
				}
				return null;
			}
			tlen += len;
			if (tlen == size) {
				buf.flip();
				return ret;
			} else if (tlen > size) {
				buf.flip();
				buf.limit(buf.limit() - ((int) (tlen - size)));
				return ret;
			} else {
				if (!buf.hasRemaining()) {
					buf = bf.getBuffer();
					ret.add(buf);
				}
			}

		}
	}

	
	public static long checksum(List<ByteBuffer> buffers,Adler32 obj){
			obj.reset();
			for(ByteBuffer buf:buffers){
				buf.mark();
				obj.update(buf);
				buf.reset();
			}
			return obj.getValue();
	}
	
	public boolean validChecksum(List<ByteBuffer> buffers,BufferFactory bf,Adler32 obj){
		int idx = buffers.size()-1;
		ByteBuffer buf = buffers.get(idx);
		long checksumVal=0;
		int newLimit = 0;
		int remaining = buf.remaining();
		
		if(remaining>8){
			newLimit = buf.limit() - 8;			
			buf.position(newLimit);			
			checksumVal = buf.getLong();
			buf.clear();
			buf.limit(newLimit);
		}else if(remaining ==8){
			checksumVal = buf.getLong();
			buffers.remove(idx);	
			bf.freeBuffer(buf);
		}else {
			byte[] tmp = new byte[8];
			ByteBuffer tmpBuf = buf;
			buf.get(tmp,8-remaining,remaining);
			buffers.remove(idx);
			--idx;
			buf = buffers.get(idx);
			buf.position(buf.limit()-(8-remaining));
			buf.get(tmp,8,8-remaining);
			buf.limit(buf.limit()-(8-remaining));
			buf.position(0);
			tmpBuf.clear();
			tmpBuf.put(tmp);
			tmpBuf.flip();
			checksumVal = tmpBuf.getLong();	
			bf.freeBuffer(buf);
		}
		return checksumVal == checksum(buffers,obj);
	}
	
	
	public static void addChecksum(List<ByteBuffer> buffers,BufferFactory bf,Adler32 obj){
		long checksum = checksum(buffers,obj);
		ByteBuffer last = buffers.get(buffers.size()-1);
		int remaining = last.capacity()- last.limit();
		if(remaining>=8){
			last.mark();
			int oldLimit = last.limit();
			last.limit(oldLimit+8);
			last.position(oldLimit);
			last.putLong(checksum);
			last.reset();
		}else{
			ByteBuffer nlast = bf.getBuffer();
			nlast.putLong(checksum);
			nlast.flip();
			buffers.add(nlast);
		}
	}
	
	

	public static byte[] readByteContent(AsynchronousFileChannel channel, long position, int size, ByteBuffer buf)
			throws Exception {
		byte[] ret = new byte[size];
		int tlen = 0, idx = 0;
		while (true) {
			int len = channel.read(buf, position + tlen).get();
			if (len == -1) {
				return null;
			}
			if (len > 0) {
				buf.flip();
				tlen += len;
				if (tlen == size) {
					buf.get(ret, idx, len);
					return ret;
				} else if (tlen > size) {
					buf.get(ret, idx, size - idx);
					return ret;
				} else {
					buf.get(ret, idx, len);
					buf.clear();
					idx = tlen;
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		Random random = new Random();
		byte[] bs = new byte[2*8092];
		for(int i = 0; i < bs.length; ++i){
			bs[i] =(byte) random.nextInt(256);
		}
		ByteBuffer buf = ByteBuffer.allocate(2*8092);
		buf.put(bs);
		//System.out.println(buf.limit()+","+buf.position());
		buf.flip();
		Adler32 daler32 = new Adler32();
		daler32.reset();
		daler32.update(buf);
		System.out.println(daler32.getValue());
		System.out.println(buf.limit()+","+buf.position());
		daler32.reset();
		buf.clear();
		buf.put(bs,0,8092);
		buf.flip();
		daler32.update(buf);
		buf.clear();
		buf.put(bs,8092,8092);
		buf.flip();
		daler32.update(buf);
		System.out.println(daler32.getValue());
		System.out.println(buf.limit()+","+buf.position());			
		
	}
}
