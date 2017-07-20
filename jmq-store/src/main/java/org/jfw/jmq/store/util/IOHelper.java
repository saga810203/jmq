package org.jfw.jmq.store.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
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
	
	
	public static void store(AsynchronousFileChannel ch,List<ByteBuffer> buffers,long position) throws InterruptedException, ExecutionException, IOException{
		int idx   = 0;
		while(idx<buffers.size()){
			ByteBuffer buf = buffers.get(idx);
			if(buf.hasRemaining()){
				int ret = ch.write(buf, position).get();
				position+=ret;
			}else{
				++idx;
			}
		}
		ch.force(false);
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
