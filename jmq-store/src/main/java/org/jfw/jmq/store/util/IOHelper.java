package org.jfw.jmq.store.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.List;
import java.util.concurrent.Future;

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
	
	
	public long readSize(AsynchronousFileChannel channel,long position,BufferFactory bf)throws Exception{
		ByteBuffer buf = bf.getBuffer();
		int tlen = 0;
		try{
			buf.limit(8);
			while(true){
				int len =channel.read(buf,position+tlen).get();
				if(len ==-1){
					return -1;
				}
				tlen +=len;
				if(tlen>=8){
					buf.flip();
					return buf.getLong();
				}
			}
		}finally{
			bf.freeBuffer(buf);
		}
	}
	public List<ByteBuffer> readCnt(AsynchronousFileChannel channel,long position,BufferFactory bf)throws Exception{
		ByteBuffer buf = bf.getBuffer();
		int tlen = 0;
		try{
			buf.limit(8);
			
			while(true){
				int len =channel.read(buf,position+tlen).get();
				if(len ==-1){
					return -1;
				}
				tlen +=len;
				if(tlen>=8){
					buf.flip();
					return buf.getLong();
				}
			}
		}finally{
			bf.freeBuffer(buf);
		}
	}

}
