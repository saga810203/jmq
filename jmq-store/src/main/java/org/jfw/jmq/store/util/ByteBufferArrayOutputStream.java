package org.jfw.jmq.store.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ByteBufferArrayOutputStream extends OutputStream {
	private BufferFactory bf;
	private ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
	private ByteBuffer last = null;

	public ByteBufferArrayOutputStream(BufferFactory bf) {
		this.bf = bf;
	}

	@Override
	public void write(byte[] b) throws IOException {
		this.write(b,0,b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if ((last == null) || (!last.hasRemaining())) {
			last = bf.getBuffer();
			buffers.add(last);
		}
		for (;;) {
			int remaining = last.remaining();
			if (remaining >= len) {
				last.put(b, off, len);
				break;
			} else {
				last.put(b, off, remaining);
				off += remaining;
				len -= remaining;
				last = bf.getBuffer();
				buffers.add(last);
			}
		}
	}


	@Override
	public void write(int b) throws IOException {
		if ((last == null) || (!last.hasRemaining())) {
			last = bf.getBuffer();
			buffers.add(last);
		}
		last.put((byte)b);
	}
	

	public List<ByteBuffer> getByteBufferList() {
		for(ByteBuffer b:this.buffers){
			b.flip();
		}
		return this.buffers;
	}

}
